package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多工作负载并发抢额度 + 同序列重放确定性。
 */
class SharedCapacityConcurrencyTest {

    static ScalePolicy eagerPolicy() {
        return new ScalePolicy(1L, 1, 100, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
    }

    /**
     * 总盘 N+1：N 个工作负载各占 1，只剩最后 1 份；N 个线程栅栏并发上报高负载并 reconcile。
     * 不变量：任何交错下只有恰好一个工作负载扩到 2，全系统存活实例总数恒等于 N+1（绝不超发）。
     */
    @Test
    void concurrentWorkloadsContendForLastSlotWithoutOvercommit() throws Exception {
        int n = 8;
        TestHarness h = new TestHarness(30_000L, n + 1);
        for (int i = 0; i < n; i++) {
            String wl = "w" + i;
            assertTrue(h.controller.registerWorkload(wl, 1));
            h.controller.updatePolicy(wl, eagerPolicy());
            // 预置高负载观测（不同版本线，各自 v1），让并发只发生在评估/占额阶段
            h.controller.observeLoad(new LoadObservation(wl, 1L, h.clock.nowMillis(), 0.95));
        }
        assertEquals(n + 1, h.controller.capacitySnapshot().totalCapacity());
        assertEquals(n, h.controller.capacitySnapshot().usedCapacity());

        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger scaledUp = new AtomicInteger();
        AtomicInteger pending = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            String wl = "w" + i;
            pool.submit(() -> {
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                ScaleDecision d = h.controller.reconcileScale(wl);
                if (d.outcome() == ScaleOutcome.SCALED_UP) {
                    scaledUp.incrementAndGet();
                } else if (d.outcome() == ScaleOutcome.CAPACITY_PENDING) {
                    pending.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        assertEquals(1, scaledUp.get(), "exactly one workload wins the last slot");
        assertEquals(n - 1, pending.get(), "all others are queued, not rejected-vaguely");

        // 全局不变量：所有工作负载实例之和 == 总盘，绝不超过
        int total = 0;
        for (int i = 0; i < n; i++) {
            total += h.controller.snapshot("w" + i).instances().size();
        }
        assertEquals(n + 1, total);
        assertEquals(n + 1, h.controller.capacitySnapshot().usedCapacity());
        assertEquals(0, h.controller.capacitySnapshot().availableCapacity());

        // 等待队列无重复条目
        List<String> q = h.controller.capacitySnapshot().waitingQueue();
        assertEquals(n - 1, q.size());
        assertEquals(n - 1, new java.util.HashSet<>(q).size());

        // 收敛：赢家缩容 1 份后，释放驱动等待者——每释放一次恰好队首补一份，最终其余 N-1 个
        // 通过“赢家反复放弃/其他释放”之外的路径不易构造；这里验证一次释放->队首补额：
        String winner = null;
        for (int i = 0; i < n; i++) {
            if (h.controller.snapshot("w" + i).instances().size() == 2) {
                winner = "w" + i;
            }
        }
        assertTrue(winner != null);
        h.controller.observeLoad(new LoadObservation(winner, 2L, h.clock.nowMillis(), 0.2));
        ScaleDecision down = h.controller.reconcileScale(winner);
        assertEquals(ScaleOutcome.SCALED_DOWN, down.outcome());
        // 释放后队首等待者补额：总占用仍是 N+1
        assertEquals(n + 1, h.controller.capacitySnapshot().usedCapacity());
        int total2 = 0;
        for (int i = 0; i < n; i++) {
            total2 += h.controller.snapshot("w" + i).instances().size();
        }
        assertEquals(n + 1, total2);
        System.out.println("[concurrency] winner=" + winner + " queueAfterGrant="
                + h.controller.capacitySnapshot().waitingQueue());
    }

    /**
     * 同一条确定性输入序列（含占满、等待、释放、轮流补额）执行两次：
     * 各工作负载最终实例数、FIFO 队列、账本占用必须逐次一致。
     */
    @Test
    void sameSequenceReplaysToIdenticalAllocation() {
        List<String> r1 = SharedCapacityConcurrencyTest.<List<String>>cast(runScenario(false));
        List<String> r2 = cast(runScenario(true));
        assertEquals(r1, r2, "identical input sequence must yield identical allocation order");
        System.out.println("[determinism] replay finals+grants equal: " + r2);
    }

    @SuppressWarnings("unchecked")
    static <T> T cast(Object o) {
        return (T) o;
    }

    /** 纯顺序、无并发线程的脚本：谁先拿、谁后拿完全由 FIFO 与事件顺序决定。 */
    static List<String> runScenario(boolean dump) {
        TestHarness h = new TestHarness(30_000L, 3);
        List<String> trace = new ArrayList<>();
        h.controller.registerWorkload("a", 2); // 占 2
        h.controller.registerWorkload("b", 1); // 占满 3
        h.controller.updatePolicy("a", eagerPolicy());
        h.controller.updatePolicy("b", eagerPolicy());
        ScalePolicy b2 = new ScalePolicy(2L, 1, 100, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);

        // a 想扩 -> 排队队首；b 想扩 -> 排队队尾
        h.controller.observeLoad(new LoadObservation("a", 1L, h.clock.nowMillis(), 0.95));
        h.controller.reconcileScale("a");
        h.controller.observeLoad(new LoadObservation("b", 1L, h.clock.nowMillis(), 0.95));
        h.controller.reconcileScale("b");
        trace.add("queue=" + h.controller.capacitySnapshot().waitingQueue());

        // b 被策略 v2 刷新（仍高负载意图不变，观测未消费）
        h.controller.updatePolicy("b", b2);

        // a 缩容释放：a 出队（最新低负载），b 补额 1->2
        h.controller.observeLoad(new LoadObservation("a", 2L, h.clock.nowMillis(), 0.2));
        h.controller.reconcileScale("a");
        trace.add("a=" + h.controller.snapshot("a").instances().size());
        trace.add("b=" + h.controller.snapshot("b").instances().size());
        trace.add("used=" + h.controller.capacitySnapshot().usedCapacity());
        trace.add("queue=" + h.controller.capacitySnapshot().waitingQueue());

        // b 也缩容，全部回落
        h.controller.observeLoad(new LoadObservation("b", 2L, h.clock.nowMillis(), 0.2));
        h.controller.reconcileScale("b");
        trace.add("b2=" + h.controller.snapshot("b").instances().size());
        trace.add("used2=" + h.controller.capacitySnapshot().usedCapacity());
        trace.add("alloc=" + h.controller.capacitySnapshot().allocations());
        if (dump) {
            h.controller.eventLog().dump();
        }
        return trace;
    }
}
