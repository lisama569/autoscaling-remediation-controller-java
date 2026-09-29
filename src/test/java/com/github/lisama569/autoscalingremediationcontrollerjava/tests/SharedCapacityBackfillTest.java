package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 额度释放后补分配的两类漏分配回归用例：
 * <ol>
 *   <li>等待者一次性欠多份：释放后必须在该次操作内轮转补到目标（或确实无额度），
 *       剩余需求既不能被丢出队列，手动补分配也必须有效；</li>
 *   <li>改策略（下调 max）腾出的额度：等待者必须当场拿到，无需不相干的后续操作带出。</li>
 * </ol>
 * 另覆盖多等待者轮转公平、逐份释放收敛与同序列重跑的确定性。
 */
class SharedCapacityBackfillTest {

    /** 便于直接驱动观测扩容的策略；min/max 由调用方指定。 */
    private static ScalePolicy policy(long version, int min, int max) {
        return new ScalePolicy(version, min, max, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
    }

    private void pushHigh(TestHarness h, String wl, long version) {
        h.controller.observeLoad(new LoadObservation(wl, version, h.clock.nowMillis(), 0.95d));
    }

    /** 等待者欠多份（策略抬 min 欠下 2 份）：一次释放 2 份后必须在同一操作内补到位并收敛。 */
    @Test
    void waiterOwingMultipleUnitsIsFullyBackfilledOnSingleRelease() {
        TestHarness h = new TestHarness(30_000L, 3);
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1); // 共 3，盘子满

        // b 策略要求 min=3：当前 1，还差 2 份；逐份申请，第一份不足即排队，剩余需求留在 desired=3
        h.controller.updatePolicy("b", policy(1L, 3, 5));
        assertEquals(1, h.controller.snapshot("b").instances().size());
        assertEquals(3, h.controller.snapshot("b").desiredReplicas());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());
        System.out.println("[backfill] b owes 2 units, queue=" + h.controller.capacitySnapshot().waitingOrder());

        // a 整体退出释放 2 份，deregister 内部必须当场把 b 补到 3
        h.controller.deregisterWorkload("a");
        assertEquals(3, h.controller.snapshot("b").instances().size(),
                "waiter owing 2 units must be fully backfilled to its target in the releasing operation");
        assertEquals(3, h.controller.snapshot("b").desiredReplicas());
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty(), "queue must be empty at convergence");
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[backfill] b fully backfilled to 3, used="
                + h.controller.capacitySnapshot().usedCapacity());
    }

    /** 欠多份但只释放 1 份：补 1 份后必须继续排队；再释放（含手动 reconcile）必须继续补直到收敛。 */
    @Test
    void partialBackfillKeepsRemainingDemandQueuedUntilConverged() {
        TestHarness h2 = new TestHarness(30_000L, 3);
        h2.controller.registerWorkload("a", 2);
        h2.controller.registerWorkload("b", 1);
        // a 可观测缩容（min=1，max=10）；b 策略抬 min=3 欠下 2 份
        h2.controller.updatePolicy("a", policy(1L, 1, 10));
        h2.controller.updatePolicy("b", policy(1L, 3, 5));
        assertEquals(List.of("b"), h2.controller.capacitySnapshot().waitingOrder());

        h2.controller.observeLoad(new LoadObservation("a", 1L, h2.clock.nowMillis(), 0.1d));
        ScaleDecision down = h2.controller.reconcileScale("a");
        assertEquals(ScaleOutcome.SCALED_DOWN, down.outcome());
        // 释放 1 份：b 补到 2，但仍欠 1 份，必须继续排队，不能被当成已满足而丢弃
        assertEquals(2, h2.controller.snapshot("b").instances().size());
        assertEquals(List.of("b"), h2.controller.capacitySnapshot().waitingOrder(),
                "remaining 1-unit demand must stay queued after a partial backfill");
        assertEquals(3, h2.controller.capacitySnapshot().usedCapacity());
        System.out.println("[backfill] b partially backfilled to 2, still queued for 1 more");

        // 再释放 1 份（a 退出）：剩余需求当场补足并收敛
        h2.controller.deregisterWorkload("a");
        assertEquals(3, h2.controller.snapshot("b").instances().size());
        assertTrue(h2.controller.capacitySnapshot().waitingOrder().isEmpty());

        // 显式手动触发补分配也必须能把排队需求带出来（旧实现中队列已被清空，这里验证不再发生）
        TestHarness h3 = new TestHarness(30_000L, 3);
        h3.controller.registerWorkload("a", 2);
        h3.controller.registerWorkload("b", 1);
        h3.controller.updatePolicy("b", policy(1L, 3, 5));
        h3.controller.deregisterWorkload("a"); // 2 份空闲
        h3.controller.reconcilePendingCapacity(); // 手动补分配同样收敛
        assertEquals(3, h3.controller.snapshot("b").instances().size(),
                "manual reconcile must grant all outstanding demand");
        assertTrue(h3.controller.capacitySnapshot().waitingOrder().isEmpty());
        System.out.println("[backfill] manual reconcile also converges b to 3");
    }

    /** 改策略下调 max 导致缩容腾出的额度：等待者必须在同一次策略写入内当场拿到。 */
    @Test
    void capacityFreedByPolicyTighteningIsGrantedToWaiterInSameOperation() {
        TestHarness h = new TestHarness(30_000L, 4);
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1);
        h.controller.registerWorkload("c", 1); // 共 4，盘子满
        h.controller.updatePolicy("a", policy(1L, 1, 10));
        h.controller.updatePolicy("b", policy(1L, 1, 10));
        h.controller.updatePolicy("c", policy(1L, 1, 10));

        // b 观测驱动想扩 1 份 -> 排队
        pushHigh(h, "b", 1L);
        assertEquals(ScaleOutcome.PENDING_CAPACITY, h.controller.reconcileScale("b").outcome());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());

        // 把 a 的上限从 10 调到 1：desired 立即钳为 1，a 终止 1 个实例并归还 1 份。
        // 这次策略写入本身就必须把刚释放的额度当场授予 b。
        h.controller.updatePolicy("a", policy(2L, 1, 1));
        assertEquals(1, h.controller.snapshot("a").instances().size(), "a tightened to max=1");
        assertEquals(2, h.controller.snapshot("b").instances().size(),
                "waiter must receive capacity freed by policy tightening in the same operation");
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty(),
                "waiter must not be left hanging until an unrelated operation");
        assertEquals(4, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[backfill] policy tightening freed 1 slot; b granted immediately to 2");
    }

    /** 多个等待者轮转：每个工作负载一次只取 1 份然后回到队尾，额度耗尽时队首继续等待。 */
    @Test
    void multipleWaitersRotateOneUnitEachAndHeadKeepsWaitingWhenPoolEmpty() {
        TestHarness h = new TestHarness(30_000L, 5);
        h.controller.registerWorkload("a", 1);
        h.controller.registerWorkload("b", 1);
        h.controller.registerWorkload("c", 1);
        h.controller.registerWorkload("d", 1);
        h.controller.registerWorkload("e", 1); // 5 份占满
        // a、b 各欠 2 份（min=3），依次排队
        h.controller.updatePolicy("a", policy(1L, 3, 5));
        h.controller.updatePolicy("b", policy(1L, 3, 5));
        assertEquals(List.of("a", "b"), h.controller.capacitySnapshot().waitingOrder());

        // d、e 退出释放 2 份：轮转下 a、b 各拿 1 份（谁也不能一次拿 2 份），额度耗尽后停止
        h.controller.deregisterWorkload("d");
        h.controller.deregisterWorkload("e");
        assertEquals(2, h.controller.snapshot("a").instances().size(), "a gets exactly 1 unit per round");
        assertEquals(2, h.controller.snapshot("b").instances().size(), "b gets exactly 1 unit per round");
        assertEquals(5, h.controller.capacitySnapshot().usedCapacity());
        // 两者都仍欠 1 份；队列为 a,b（轮转后保持确定顺序）
        assertEquals(List.of("a", "b"), h.controller.capacitySnapshot().waitingOrder(),
                "both waiters keep waiting, neither hogs freed capacity");

        // c 退出再释放 1 份：队首 a 先补足并离队，b 仍在队首等待
        h.controller.deregisterWorkload("c");
        assertEquals(3, h.controller.snapshot("a").instances().size());
        assertEquals(2, h.controller.snapshot("b").instances().size());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());

        // b 最后一份需求随后释放即补足
        h.controller.deregisterWorkload("a"); // 释放 a 剩余份额，b 拿 1 份后收敛
        assertEquals(3, h.controller.snapshot("b").instances().size());
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty());
        System.out.println("[backfill] rotation fair: a,b each took 1/round then converged in FIFO order");
    }

    /** 同一输入序列重复执行：补分配轨迹（谁先拿、各拿几份、队列顺序）逐次一致。 */
    @Test
    void backfillSequenceIsDeterministicAcrossRuns() {
        List<String> run1 = runBackfillScenario();
        List<String> run2 = runBackfillScenario();
        assertEquals(run1, run2, "backfill trace must be identical across repeated runs");
        System.out.println("[backfill-determinism] trace=" + run2);
    }

    private List<String> runBackfillScenario() {
        TestHarness h = new TestHarness(30_000L, 4);
        List<String> trace = new ArrayList<>();
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1);
        h.controller.registerWorkload("c", 1);
        h.controller.updatePolicy("a", policy(1L, 1, 10));
        h.controller.updatePolicy("b", policy(1L, 3, 5)); // b 欠 2 份
        h.controller.updatePolicy("c", policy(1L, 1, 10));
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());

        // 策略下调 a 的 max 释放 1 份：b 当场补 1 份，仍欠 1 份
        h.controller.updatePolicy("a", policy(2L, 1, 1));
        trace.add("a-size:" + h.controller.snapshot("a").instances().size());
        trace.add("b-size:" + h.controller.snapshot("b").instances().size());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());
        trace.add("used:" + h.controller.capacitySnapshot().usedCapacity());

        // c 退出再释放 1 份：b 补足收敛
        h.controller.deregisterWorkload("c");
        trace.add("b-size:" + h.controller.snapshot("b").instances().size());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());
        trace.add("used:" + h.controller.capacitySnapshot().usedCapacity());
        return trace;
    }
}
