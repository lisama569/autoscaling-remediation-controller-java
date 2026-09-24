package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ObservationAcceptResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 观测版本治理与并发收敛：
 * 乱序/重复观测不得覆盖状态；并发重复事件下不得超额扩容或漏扩容。
 */
class ObservationAndConcurrencyTest {

    @Test
    void duplicateAndOutOfOrderObservationsAreRejected() {
        TestHarness h = new TestHarness();
        String wl = "wl-obs";
        h.controller.registerWorkload(wl, 1);
        h.controller.updatePolicy(wl, TestHarness.defaultPolicy());
        long now = h.clock.nowMillis();

        assertTrue(h.controller.observeLoad(new LoadObservation(wl, 5L, now, 0.9)).accepted());
        ObservationAcceptResult dup = h.controller.observeLoad(new LoadObservation(wl, 5L, now + 1, 0.1));
        assertEquals(RejectionReason.POLICY_VERSION_CONFLICT, dup.rejectionReason());
        ObservationAcceptResult rollback = h.controller.observeLoad(new LoadObservation(wl, 4L, now + 2, 0.1));
        assertEquals(RejectionReason.POLICY_VERSION_CONFLICT, rollback.rejectionReason());
        // 未被旧观测覆盖：仍是 v5、ratio=0.9
        WorkloadState state = h.controller.snapshot(wl);
        assertEquals(5L, state.latestLoadVersion());
        assertEquals(0.9, state.latestLoad().loadRatio(), 1e-9);
        System.out.println("[version] duplicate/rollback rejected, latest stays v5 ratio=0.9");

        // 乱序健康观测：每个实例独立版本线
        h.controller.observeHealth(new HealthObservation(wl, "i1", 2L, now, false));
        ObservationAcceptResult healthOld =
                h.controller.observeHealth(new HealthObservation(wl, "i1", 1L, now + 1, true));
        assertEquals(RejectionReason.POLICY_VERSION_CONFLICT, healthOld.rejectionReason());
        System.out.println("[version] health rollback rejected for i1");
        h.controller.eventLog().dump();
    }

    /**
     * 同一高负载观测被 N 个线程并发“接收 + 伸缩评估”。
     * 期望：只有 stabilizationRounds 累计到阈值的那一次发生扩容，且只扩 1 个副本，
     * 不因重复事件出现超额扩容；其余为 NO_CHANGE / IN_COOLDOWN。
     */
    @Test
    void concurrentDuplicateScalingTriggersConverge() throws Exception {
        TestHarness h = new TestHarness();
        String wl = "wl-conc-scale";
        h.controller.registerWorkload(wl, 1);
        // rounds=1, cooldown=0：语义上每轮最多扩 1；同一条观测（同版本）只能被接收一次
        ScalePolicy policy = new ScalePolicy(1L, 1, 5, 0.8, 0.5, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
        h.controller.updatePolicy(wl, policy);

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger scaledUp = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            long version = t + 1L;
            pool.submit(() -> {
                await(start);
                // 每个线程带不同版本但相同内容：无论交错如何，最终状态必须一致且无超额
                ObservationAcceptResult r = h.controller.observeLoad(
                        new LoadObservation(wl, version, h.clock.nowMillis(), 0.95));
                if (!r.accepted()) {
                    conflicts.incrementAndGet();
                }
                ScaleDecision d = h.controller.reconcileScale(wl);
                if (d.outcome() == ScaleOutcome.SCALED_UP) {
                    scaledUp.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        // rounds=1, cooldown=0, 32 条高负载（无论什么顺序接收），上限 5、起点 1 -> 恰好扩容 4 次
        assertEquals(4, scaledUp.get(), "exactly 4 scale-ups, got " + scaledUp.get());
        WorkloadState state = h.controller.snapshot(wl);
        assertEquals(5, state.desiredReplicas());
        assertEquals(5, state.instances().size());
        System.out.println("[concurrency] scaledUp=" + scaledUp.get()
                + " versionConflicts=" + conflicts.get()
                + " finalInstances=" + state.instances().size());
        h.controller.eventLog().dump();
    }

    /**
     * 重复版本 + 重复评估混合：同一条观测被反复评估，已据此扩过容后不得重复扩容。
     */
    @Test
    void sameObservationReEvaluatedDoesNotDoubleScale() throws Exception {
        TestHarness h = new TestHarness();
        String wl = "wl-double";
        h.controller.registerWorkload(wl, 1);
        ScalePolicy policy = new ScalePolicy(1L, 1, 5, 0.8, 0.5, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
        h.controller.updatePolicy(wl, policy);
        h.controller.observeLoad(new LoadObservation(wl, 1L, h.clock.nowMillis(), 0.95));

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger scaledUp = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                await(start);
                ScaleDecision d = h.controller.reconcileScale(wl); // 同一条 v1 观测反复评估
                if (d.outcome() == ScaleOutcome.SCALED_UP) {
                    scaledUp.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        // 同一条 v1 观测无论被并发评估多少次，至多触发一次伸缩：1 -> 2
        assertEquals(1, scaledUp.get(), "exactly one scale-up for one observation, got " + scaledUp.get());
        assertEquals(2, h.controller.snapshot(wl).desiredReplicas());
        System.out.println("[no-double] scaleUp calls=" + scaledUp.get()
                + " final=" + h.controller.snapshot(wl).desiredReplicas());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
