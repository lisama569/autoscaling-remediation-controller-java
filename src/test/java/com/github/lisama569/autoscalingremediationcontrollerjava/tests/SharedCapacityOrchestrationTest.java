package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RegistrationResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多工作负载共享容量协同编排：
 * <ol>
 *   <li>一个工作负载占满额度，另一个只能等待，后来拿到额度；</li>
 *   <li>缩容/退出释放额度后按 FIFO 重新分配，最终收敛；</li>
 *   <li>多个工作负载并发抢最后一份额度，总数绝不超限，赢家确定后其余等待；</li>
 *   <li>同一输入序列重跑，分配先后与最终收敛完全一致（确定性）。</li>
 * </ol>
 */
class SharedCapacityOrchestrationTest {

    /** rounds=1、cooldown=0、min=1、max=10，便于直接驱动每次高负载扩 1。 */
    private static ScalePolicy eagerPolicy(long version) {
        return new ScalePolicy(version, 1, 10, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
    }

    private void pushHigh(TestHarness h, String wl, long version) {
        h.controller.observeLoad(new LoadObservation(wl, version, h.clock.nowMillis(), 0.95d));
    }

    private void pushLow(TestHarness h, String wl, long version) {
        h.controller.observeLoad(new LoadObservation(wl, version, h.clock.nowMillis(), 0.1d));
    }

    @Test
    void oneWorkloadFillsPoolAnotherWaitsThenGetsCapacity() {
        TestHarness h = new TestHarness(30_000L, 3); // 全局仅 3 份
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1); // 共 3，盘子满
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());

        // a 先占坑：再来高负载也扩不动，因为已在天花板 3（allocation==active==2，max 内但盘子满）
        pushHigh(h, "a", 1L);
        ScaleDecision aWant = h.controller.reconcileScale("a");
        // a 当前 2，目标 3 但盘子空：PENDING_CAPACITY
        assertEquals(ScaleOutcome.PENDING_CAPACITY, aWant.outcome());
        assertEquals(RejectionReason.SHARED_CAPACITY_EXHAUSTED, aWant.rejectionReason());
        assertEquals(2, h.controller.snapshot("a").instances().size());
        System.out.println("[shared] a scale-up -> " + aWant.outcome() + " : " + aWant.rationale());

        // b 也想扩：a 已是队首，b 不能插队（且本来也没额度）
        pushHigh(h, "b", 1L);
        ScaleDecision bWant = h.controller.reconcileScale("b");
        assertEquals(ScaleOutcome.PENDING_CAPACITY, bWant.outcome());
        assertEquals(List.of("a", "b"), h.controller.capacitySnapshot().waitingOrder());
        System.out.println("[shared] b queue behind a: " + h.controller.capacitySnapshot().waitingOrder());

        // b 缩容不现实（b 只有 1 = min）。让 a 整体退出释放 2 份
        h.controller.deregisterWorkload("a");
        // 退出触发 drain：b 是唯一等待者，应补到它的目标。先看 b 是否拿到 1 份（2 份空闲）
        assertEquals(2, h.controller.snapshot("b").instances().size(),
                "b should have received freed capacity");
        // b 此前只申请过 1 份（一次高负载决策），收敛到 2
        assertEquals(2, h.controller.snapshot("b").desiredReplicas());
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty());
        System.out.println("[shared] after a exits, b grows to "
                + h.controller.snapshot("b").instances().size() + " and queue drains");
        h.controller.eventLog().dump();
    }

    @Test
    void releasedCapacityIsReallocatedFifoAndConverges() {
        TestHarness h = new TestHarness(30_000L, 3);
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1);
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));

        // b 先排队（a 当前不想扩）
        pushHigh(h, "b", 1L);
        assertEquals(ScaleOutcome.PENDING_CAPACITY, h.controller.reconcileScale("b").outcome());
        // a 缩容：先制造 a=2 -> 低负载两轮？a min=1，一次低负载即可从 2 降到 1
        pushLow(h, "a", 1L);
        ScaleDecision down = h.controller.reconcileScale("a");
        assertEquals(ScaleOutcome.SCALED_DOWN, down.outcome());
        // 缩容后自动 drain：b 拿到释放的 1 份
        assertEquals(2, h.controller.snapshot("b").instances().size());
        assertEquals(1, h.controller.snapshot("a").instances().size());
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty());
        System.out.println("[shared] a scaled down, freed slot reallocated to b (FIFO)");
    }

    @Test
    void registerBeyondCapacityIsRejectedCleanly() {
        TestHarness h = new TestHarness(30_000L, 3);
        RegistrationResult ok = h.controller.registerWorkload("a", 2);
        assertTrue(ok.registered());
        RegistrationResult reject = h.controller.registerWorkload("b", 2); // 只剩 1
        assertFalse(reject.registered());
        assertEquals(RejectionReason.SHARED_CAPACITY_EXHAUSTED, reject.rejectionReason());
        // 没有残留：b 未注册，额度仍为 2
        assertFalse(h.registry.isRegistered("b"));
        assertEquals(2, h.controller.capacitySnapshot().usedCapacity());
        // 释放后可以重新注册成功
        h.controller.deregisterWorkload("a");
        assertTrue(h.controller.registerWorkload("b", 2).registered());
        System.out.println("[shared] over-capacity register rejected cleanly, retry after release ok");
    }

    @Test
    void concurrentContentionForLastUnitNeverOvercommits() throws Exception {
        TestHarness h = new TestHarness(30_000L, 3);
        h.controller.registerWorkload("a", 1);
        h.controller.registerWorkload("b", 1);
        h.controller.registerWorkload("c", 1); // 3 份占满
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));
        h.controller.updatePolicy("c", eagerPolicy(1L));

        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final String wl = (t % 3 == 0) ? "a" : (t % 3 == 1) ? "b" : "c";
            final long v = t + 1L;
            pool.submit(() -> {
                await(start);
                h.controller.observeLoad(new LoadObservation(wl, v, h.clock.nowMillis(), 0.95d));
                h.controller.reconcileScale(wl);
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        // 不变量：无论怎么交错，活动实例总数绝不超过 3
        int total = h.controller.snapshot("a").instances().size()
                + h.controller.snapshot("b").instances().size()
                + h.controller.snapshot("c").instances().size();
        assertEquals(3, total, "total active instances must never exceed shared capacity");
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[shared-concurrency] totalActive=" + total
                + " used=" + h.controller.capacitySnapshot().usedCapacity()
                + " waiting=" + h.controller.capacitySnapshot().waitingOrder());
    }

    @Test
    void concurrentRemediationAndScalingAcrossWorkloadsConvergeWithNoStuckTasks() throws Exception {
        TestHarness h = new TestHarness(30_000L, 6);
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 2);
        h.controller.registerWorkload("c", 2); // 6 份占满
        // 开启修复的策略
        ScalePolicy pa = new ScalePolicy(1L, 1, 10, 0.8d, 0.5d, 1, 0L,
                true, 2, 5, 10_000L, 0L, 1_000L);
        for (String wl : List.of("a", "b", "c")) {
            h.controller.updatePolicy(wl, pa);
        }

        int threads = 30;
        ExecutorService pool = Executors.newFixedThreadPool(10);
        CountDownLatch start = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger seq = new java.util.concurrent.atomic.AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final String wl = List.of("a", "b", "c").get(t % 3);
            final boolean healthEvent = (t % 2 == 0);
            pool.submit(() -> {
                await(start);
                int n = seq.incrementAndGet();
                if (healthEvent) {
                    // 对实例 1 交替上报不健康/健康（版本单调）
                    boolean healthy = n % 4 == 0;
                    h.controller.observeHealth(new HealthObservation(wl, wl + "-inst-1",
                            n, h.clock.nowMillis(), healthy));
                } else {
                    h.controller.observeLoad(new LoadObservation(wl, n, h.clock.nowMillis(), 0.95d));
                    h.controller.reconcileScale(wl);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        // 推进修复直到没有活动任务（有界推进，防止真卡死时无限循环）
        int rounds = 0;
        while (rounds++ < 50) {
            h.advance(50L);
            h.controller.tickRemediations();
            if (countActiveRemediations(h) == 0) {
                break;
            }
        }

        // 不变量 1：没有卡住/重复的活动修复任务
        assertEquals(0, countActiveRemediations(h), "no PENDING/RUNNING tasks remain");
        for (String wl : List.of("a", "b", "c")) {
            // 每个实例至多一条任务历史（无重复任务）
            WorkloadState s = h.controller.snapshot(wl);
            long distinctInstances = s.remediations().stream()
                    .map(rt -> rt.getInstanceId()).distinct().count();
            assertEquals(distinctInstances, s.remediations().size(),
                    "no duplicate active tasks per instance in " + wl);
        }
        // 不变量 2：活动实例总数绝不超过共享上限
        int total = h.controller.snapshot("a").instances().size()
                + h.controller.snapshot("b").instances().size()
                + h.controller.snapshot("c").instances().size();
        assertTrue(total <= 6, "total instances " + total + " <= capacity 6");
        assertEquals(total, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[shared-mixed] converged: totalInstances=" + total
                + " used=" + h.controller.capacitySnapshot().usedCapacity()
                + " waiting=" + h.controller.capacitySnapshot().waitingOrder()
                + " activeTasks=0");
    }

    private static long countActiveRemediations(TestHarness h) {
        long n = 0;
        for (String wl : List.of("a", "b", "c")) {
            WorkloadState s = h.controller.snapshot(wl);
            if (s != null) {
                n += s.remediations().stream()
                        .filter(rt -> rt.getStatus() == RemediationStatus.PENDING
                                || rt.getStatus() == RemediationStatus.RUNNING).count();
            }
        }
        return n;
    }

    @Test
    void blockedScaleDownKeepsCapacityUntilInstanceTerminates() {
        TestHarness h = new TestHarness(30_000L, 3);
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1);
        ScalePolicy pa = new ScalePolicy(1L, 1, 10, 0.8d, 0.5d, 1, 0L,
                true, 1, 5, 10_000L, 0L, 2_000L); // 修复超时 2000ms，便于让任务保持 RUNNING
        h.controller.updatePolicy("a", pa);
        h.controller.updatePolicy("b", eagerPolicy(1L));

        // 1) a 缩到 1：回收健康的 inst-2，释放 1 份（drain 时尚无等待者）
        pushLow(h, "a", 1L);
        assertEquals(ScaleOutcome.SCALED_DOWN, h.controller.reconcileScale("a").outcome());
        assertEquals(1, h.controller.snapshot("a").instances().size());

        // 2) c 占住最后一份（a=1,b=1,c=1，盘子满），随后 b 想扩 -> 排队
        h.controller.registerWorkload("c", 1);
        pushHigh(h, "b", 1L);
        assertEquals(ScaleOutcome.PENDING_CAPACITY, h.controller.reconcileScale("b").outcome());

        // 3) a 唯一实例不健康并进入修复（RUNNING），此时 a 想缩也无实例可回收
        h.controller.observeHealth(new HealthObservation("a", "a-inst-1", 1L,
                h.clock.nowMillis(), false));
        // a 的 min=1，已在 1，不会再缩；直接验证账实一致：used==真实活动实例总数
        int activeTotal = h.controller.snapshot("a").instances().size()
                + h.controller.snapshot("b").instances().size()
                + h.controller.snapshot("c").instances().size();
        assertEquals(3, activeTotal);
        assertEquals(activeTotal, h.controller.capacitySnapshot().usedCapacity(),
                "capacity must never be released before real termination");

        // 4) c 退出释放 1 份 -> drain 给队首 b（a 的修复中实例不受影响）
        h.controller.deregisterWorkload("c");
        assertEquals(2, h.controller.snapshot("b").instances().size());
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty());
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[shared-blocked] no early release; b granted after c exits; used="
                + h.controller.capacitySnapshot().usedCapacity());
    }

    /** 确定性：同一输入序列在全新环境重跑两次，最终分配、等待顺序、实例数完全一致。 */
    @Test
    void sameInputSequenceYieldsSameAllocationAcrossRuns() {
        List<String> run1 = runScenario(1);
        List<String> run2 = runScenario(2);
        assertEquals(run1, run2, "allocation trace must be identical across repeated runs");
        System.out.println("[shared-determinism] trace=" + run2);
    }

    /** 用脚本化的固定操作序列产出一条「分配轨迹」，用于跨运行比较。 */
    private List<String> runScenario(int runId) {
        TestHarness h = new TestHarness(30_000L, 3);
        List<String> trace = new ArrayList<>();
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1);
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));

        // b 想扩 -> 排队
        pushHigh(h, "b", 1L);
        trace.add("b-reconcile:" + h.controller.reconcileScale("b").outcome());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());

        // a 想扩 -> 也排队，排在 b 后面（FIFO）
        pushHigh(h, "a", 1L);
        trace.add("a-reconcile:" + h.controller.reconcileScale("a").outcome());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());

        // a 缩容释放 1 份 -> 队首 b 拿到
        pushLow(h, "a", 2L);
        trace.add("a-down:" + h.controller.reconcileScale("a").outcome());
        trace.add("a-size:" + h.controller.snapshot("a").instances().size());
        trace.add("b-size:" + h.controller.snapshot("b").instances().size());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());

        // a 整体退出，剩余额度重新分配；a 仍在等待（它之前想扩），退出后无人等待
        h.controller.deregisterWorkload("a");
        trace.add("b-size-after-exit:" + h.controller.snapshot("b").instances().size());
        trace.add("used:" + h.controller.capacitySnapshot().usedCapacity());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());
        return trace;
    }

    @Test
    void pendingWorkloadLeavesQueueWhenDemandDisappears() {
        TestHarness h = new TestHarness(30_000L, 3);
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1);
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));

        // a 与 b 先后排队
        pushHigh(h, "a", 1L);
        assertEquals(ScaleOutcome.PENDING_CAPACITY, h.controller.reconcileScale("a").outcome());
        pushHigh(h, "b", 1L);
        assertEquals(ScaleOutcome.PENDING_CAPACITY, h.controller.reconcileScale("b").outcome());
        assertEquals(List.of("a", "b"), h.controller.capacitySnapshot().waitingOrder());

        // a 的负载回落到滞后带（0.6 位于 (0.5, 0.8]）：扩容意向消失，应退出队列
        h.controller.observeLoad(new LoadObservation("a", 2L, h.clock.nowMillis(), 0.6d));
        assertEquals(ScaleOutcome.NO_CHANGE, h.controller.reconcileScale("a").outcome());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());
        System.out.println("[shared] a leaves queue on hysteresis; head now b: "
                + h.controller.capacitySnapshot().waitingOrder());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
