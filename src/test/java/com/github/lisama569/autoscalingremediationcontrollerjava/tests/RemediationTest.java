package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationRequestOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationRequestResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationTask;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 自愈编排：
 * 自动触发、重复请求幂等、并发上限、失败预算耗尽与窗口恢复、超时终止、实例冷却、
 * 健康抖动自然恢复，以及恢复后无重复任务/无无限重试。
 */
class RemediationTest {

    private ScalePolicy policy(int concurrency, int budget, long budgetWindow,
                               long cooldown, long timeout) {
        return new ScalePolicy(1L, 1, 10, 0.8, 0.5, 1, 500L,
                true, concurrency, budget, budgetWindow, cooldown, timeout);
    }

    private String instanceId(TestHarness h, String wl, int index) {
        return wl + "-inst-" + index;
    }

    @Test
    void unhealthyObservationAutoRemediatesAndReplacesOnSecondAttempt() {
        TestHarness h = new TestHarness();
        String wl = "wl-auto";
        h.controller.registerWorkload(wl, 1);
        h.controller.updatePolicy(wl, policy(1, 2, 10_000L, 0L, 1_000L));
        String id = instanceId(h, wl, 1);

        // 第一次修复脚本化失败
        h.infra.scriptRepair(id, false, false, 100L);
        // 健康抖动：unhealthy 事件自动建任务
        h.controller.observeHealth(new HealthObservation(wl, id, 1L, h.clock.nowMillis(), false));
        WorkloadState s1 = h.controller.snapshot(wl);
        assertEquals(1, s1.remediations().size());
        assertEquals(RemediationStatus.RUNNING, s1.remediations().get(0).getStatus());

        h.advance(100L);
        h.controller.tickRemediations();
        RemediationTask failed = h.controller.snapshot(wl).remediations().get(0);
        assertEquals(RemediationStatus.FAILED, failed.getStatus());
        assertEquals(InstanceStatus.UNHEALTHY,
                h.controller.snapshot(wl).instances().get(0).getStatus());
        System.out.println("[remediate] attempt1 FAILED mode=" + failed.getMode());

        // 第二次请求 -> REPLACE；脚本化成功 -> 替换实例出现且 READY
        h.infra.scriptRepair(id, true, false, 100L);
        RemediationRequestOutcome replaced = h.controller.requestRemediation(wl, id);
        assertEquals(RemediationRequestResult.ACCEPTED, replaced.result());
        h.advance(100L);
        h.controller.tickRemediations();

        WorkloadState end = h.controller.snapshot(wl);
        assertEquals(1, end.instances().size());
        Instance fresh = end.instances().get(0);
        assertEquals(id + "-repl", fresh.getId());
        assertEquals(InstanceStatus.READY, fresh.getStatus());
        assertEquals(RemediationStatus.SUCCEEDED, end.remediations().get(1).getStatus());
        System.out.println("[remediate] attempt2 SUCCESS replacement=" + fresh.getId());

        // 无活动任务残留、任务历史恰好两条（无无限重试）
        long active = end.remediations().stream().filter(RemediationTask::isActive).count();
        assertEquals(0, active);
        assertEquals(2, end.remediations().size());
        h.controller.eventLog().dump();
    }

    @Test
    void duplicateConcurrentRequestsCreateSingleTask() throws Exception {
        TestHarness h = new TestHarness();
        String wl = "wl-dup";
        h.controller.registerWorkload(wl, 1);
        h.controller.updatePolicy(wl, policy(1, 5, 10_000L, 0L, 1_000L));
        String id = instanceId(h, wl, 1);
        // 让修复长时间运行，请求时任务会保持 RUNNING
        h.infra.scriptRepair(id, true, false, 1_000L);
        h.controller.observeHealth(new HealthObservation(wl, id, 1L, h.clock.nowMillis(), false));

        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                RemediationRequestOutcome r = h.controller.requestRemediation(wl, id);
                if (r.result() == RemediationRequestResult.ACCEPTED) {
                    accepted.incrementAndGet();
                } else if (r.result() == RemediationRequestResult.DUPLICATE) {
                    duplicate.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        // 自动触发已创建 1 个任务：之后的显式请求全部 DUPLICATE
        assertEquals(0, accepted.get());
        assertEquals(threads, duplicate.get());
        assertEquals(1, h.controller.snapshot(wl).remediations().size());
        System.out.println("[duplicate] accepted=" + accepted.get() + " duplicate=" + duplicate.get()
                + " tasks=" + h.controller.snapshot(wl).remediations().size());
    }

    @Test
    void concurrencyCapBudgetExhaustionTimeoutAndRecovery() {
        TestHarness h = new TestHarness();
        String wl = "wl-budget";
        // 并发上限 2、预算 2（窗口 10s）、实例冷却 300ms、超时 1000ms
        h.controller.registerWorkload(wl, 5);
        h.controller.updatePolicy(wl, policy(2, 2, 10_000L, 300L, 1_000L));
        String i1 = instanceId(h, wl, 1);
        String i2 = instanceId(h, wl, 2);
        String i3 = instanceId(h, wl, 3);

        // i1/i2 修复都失败并长时间占用槽
        h.infra.scriptRepair(i1, false, false, 800L);
        h.infra.scriptRepair(i2, false, false, 800L);
        // 手动把三个实例置为不健康（绕过健康事件自动触发，便于控制顺序）
        markUnhealthy(h, wl, i1, 1L);
        markUnhealthy(h, wl, i2, 1L);
        markUnhealthy(h, wl, i3, 1L);

        assertEquals(RemediationRequestResult.ACCEPTED, h.controller.requestRemediation(wl, i1).result());
        assertEquals(RemediationRequestResult.ACCEPTED, h.controller.requestRemediation(wl, i2).result());

        // 并发上限：第三个任务被拒绝（原因可区分）
        RemediationRequestOutcome capped = h.controller.requestRemediation(wl, i3);
        assertEquals(RejectionReason.REMEDIATION_CONCURRENCY_LIMIT, capped.rejectionReason());
        System.out.println("[cap] " + capped.rationale());

        // 让 i1/i2 失败完成 -> 预算耗尽
        h.advance(800L);
        h.controller.tickRemediations();
        RemediationRequestOutcome exhausted = h.controller.requestRemediation(wl, i3);
        assertEquals(RejectionReason.FAILURE_BUDGET_EXHAUSTED, exhausted.rejectionReason());
        System.out.println("[budget] " + exhausted.rationale());

        // i1 即使健康事件再来也无法修复：预算耗尽
        var autoAfterBudget = h.controller.observeHealth(
                new HealthObservation(wl, i1, 2L, h.clock.nowMillis(), false));
        assertTrue(autoAfterBudget.accepted()); // 观测被接受
        RemediationRequestOutcome stillExhausted = h.controller.requestRemediation(wl, i1);
        assertEquals(RejectionReason.FAILURE_BUDGET_EXHAUSTED, stillExhausted.rejectionReason());

        // 实例冷却也可区分：预算恢复后立即重试 i1 仍受实例冷却（finishedAt=800, now=1000 wait=200<300）
        h.advance(200L);
        // 预算窗口未过：仍耗尽
        assertEquals(RejectionReason.FAILURE_BUDGET_EXHAUSTED,
                h.controller.requestRemediation(wl, i1).rejectionReason());

        // 越过预算窗口 + 冷却窗口：恢复；i1 的新任务可成功
        h.advance(9_801L); // now=10_801，距 finish 10_001ms > 窗口，也 > 冷却
        h.infra.scriptRepair(i1, true, false, 100L);
        RemediationRequestOutcome recovered = h.controller.requestRemediation(wl, i1);
        assertEquals(RemediationRequestResult.ACCEPTED, recovered.result());
        h.advance(100L);
        h.controller.tickRemediations();
        RemediationTask recoveredTask = lastTask(h, wl, i1);
        assertEquals(RemediationStatus.SUCCEEDED, recoveredTask.getStatus());

        // 恢复后：任务历史无 PENDING/RUNNING 残留，数量有限（无无限重试）
        WorkloadState end = h.controller.snapshot(wl);
        assertEquals(0, end.remediations().stream().filter(RemediationTask::isActive).count());
        System.out.println("[recovery] i1 task status=" + recoveredTask.getStatus()
                + " totalTasks=" + end.remediations().size());
        h.controller.eventLog().dump();
    }

    @Test
    void timedOutRemediationTerminatesWithDistinctStatus() {
        TestHarness h = new TestHarness();
        String wl = "wl-timeout";
        h.controller.registerWorkload(wl, 1);
        h.controller.updatePolicy(wl, policy(1, 5, 10_000L, 0L, 500L));
        String id = instanceId(h, wl, 1);
        markUnhealthy(h, wl, id, 1L);
        // 计划耗时 2000 > 超时 500
        h.infra.scriptRepair(id, true, false, 2_000L);
        assertEquals(RemediationRequestResult.ACCEPTED, h.controller.requestRemediation(wl, id).result());

        h.advance(500L);
        h.controller.tickRemediations();
        RemediationTask task = h.controller.snapshot(wl).remediations().get(0);
        assertEquals(RemediationStatus.TIMED_OUT, task.getStatus());
        assertEquals(InstanceStatus.UNHEALTHY, h.controller.snapshot(wl).instances().get(0).getStatus());
        System.out.println("[timeout] task terminated: " + task.getDetail());

        // 显式 timedOut 脚本同样超时
        h.advance(1L);
        h.infra.scriptRepair(id, false, true, 10L);
        RemediationRequestOutcome second = h.controller.requestRemediation(wl, id);
        assertEquals(RemediationRequestResult.ACCEPTED, second.result());
        h.advance(10L);
        h.controller.tickRemediations();
        List<RemediationTask> tasks = h.controller.snapshot(wl).remediations();
        assertEquals(RemediationStatus.TIMED_OUT, tasks.get(1).getStatus());
        System.out.println("[timeout] scripted timeout distinct from FAILED; tasks=" + tasks.size());
        h.controller.eventLog().dump();
    }

    @Test
    void healthJitterAndDisabledPolicyRejections() {
        TestHarness h = new TestHarness();
        String wl = "wl-jitter";
        h.controller.registerWorkload(wl, 1);
        ScalePolicy disabled = new ScalePolicy(1L, 1, 5, 0.8, 0.5, 1, 0L,
                false, 1, 2, 10_000L, 0L, 1_000L);
        h.controller.updatePolicy(wl, disabled);
        String id = instanceId(h, wl, 1);

        // 策略禁止：自动触发被拒，实例保持 UNHEALTHY
        h.controller.observeHealth(new HealthObservation(wl, id, 1L, h.clock.nowMillis(), false));
        RemediationRequestOutcome denied = h.controller.requestRemediation(wl, id);
        assertEquals(RejectionReason.REMEDIATION_DISABLED, denied.rejectionReason());
        System.out.println("[jitter] disabled -> " + denied.rationale());

        // 健康抖动恢复：新 healthy 观测使实例自然回到 READY，无任务产生
        h.controller.observeHealth(new HealthObservation(wl, id, 2L, h.clock.nowMillis(), true));
        WorkloadState state = h.controller.snapshot(wl);
        assertEquals(InstanceStatus.READY, state.instances().get(0).getStatus());
        assertEquals(0, state.remediations().size());
        System.out.println("[jitter] healthy observation restored instance without remediation");

        // 启用策略后，对健康实例请求修复 -> 明确拒绝（区别于策略禁止）
        h.controller.updatePolicy(wl, new ScalePolicy(2L, 1, 5, 0.8, 0.5, 1, 0L,
                true, 1, 2, 10_000L, 0L, 1_000L));
        assertEquals(RejectionReason.INSTANCE_HEALTHY,
                h.controller.requestRemediation(wl, id).rejectionReason());
        System.out.println("[jitter] healthy instance remediation rejected distinctly");
    }

    private void markUnhealthy(TestHarness h, String wl, String id, long version) {
        h.controller.observeHealth(new HealthObservation(wl, id, version, h.clock.nowMillis(), true));
        Instance instance = h.controller.snapshot(wl).instances().stream()
                .filter(i -> i.getId().equals(id)).findFirst().orElseThrow();
        instance.setStatus(InstanceStatus.UNHEALTHY);
    }

    private RemediationTask lastTask(TestHarness h, String wl, String instanceId) {
        return h.controller.snapshot(wl).remediations().stream()
                .filter(t -> t.getInstanceId().equals(instanceId))
                .reduce((a, b) -> b).orElseThrow();
    }
}
