package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.PolicyUpdateResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 额度释放后的补分配回归测试（针对两类曾漏分配的缺陷）：
 * <ol>
 *   <li>等待者一次欠多份额度：额度释放后必须在同一轮 drain 中按轮转补到位，
 *       补不齐就留在队列继续等，绝不静默丢需求；</li>
 *   <li>改策略腾出的额度（如下调 maxReplicas 触发缩容）：必须在本次操作内
 *       当场分给等待者，不能挂在队列里等不相干的操作带出来；</li>
 *   <li>多个等待者都欠多份时按 FIFO + 轮转分配，同一输入序列重跑结果逐字节一致。</li>
 * </ol>
 */
class SharedCapacityReallocationTest {

    /** rounds=1、cooldown=0、min=1、max=10。 */
    private static ScalePolicy eagerPolicy(long version) {
        return new ScalePolicy(version, 1, 10, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
    }

    /** 抬高 minReplicas 到 min 的策略（用于制造「一次欠多份」的需求）。 */
    private static ScalePolicy minPolicy(long version, int min) {
        return new ScalePolicy(version, min, 10, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
    }

    /**
     * 等待者欠 3 份额度：别的负载整体退出腾出 3 份后，必须在这次 drain 中
     * 一次性补到目标 5 并离开队列；之后再手动触发 drain 也拿不到东西（无残留需求）。
     */
    @Test
    void waiterOwingMultipleUnitsIsToppedUpInOneDrainAndConverges() {
        TestHarness h = new TestHarness(30_000L, 5); // 全局 5 份
        h.controller.registerWorkload("a", 3);
        h.controller.registerWorkload("b", 2); // 盘子满
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));

        // b 抬高 min 到 5：目标 5 但只有 2 份额度 -> 欠 3 份，进入等待队列
        PolicyUpdateResult raised = h.controller.updatePolicy("b", minPolicy(2L, 5));
        assertTrue(raised.accepted());
        assertEquals(2, h.controller.snapshot("b").instances().size());
        assertEquals(5, h.controller.snapshot("b").desiredReplicas());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());
        System.out.println("[realloc] b owes 3 units, queued: "
                + h.controller.capacitySnapshot().waitingOrder());

        // a 整体退出腾出 3 份 -> b 应在这次 drain 中补满到 5，而不是只拿 1 份
        h.controller.deregisterWorkload("a");
        assertEquals(5, h.controller.snapshot("b").instances().size(),
                "waiter owing 3 units must be topped up to its target in one drain");
        assertEquals(5, h.controller.snapshot("b").desiredReplicas());
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty());
        assertEquals(5, h.controller.capacitySnapshot().usedCapacity());

        // 需求已收敛：手动再触发补分配应一无所获（无残留、无僵尸等待）
        assertTrue(h.controller.reconcilePendingCapacity().isEmpty());
        assertEquals(5, h.controller.snapshot("b").instances().size());
        System.out.println("[realloc] b topped up to 5 in one drain; queue empty; used="
                + h.controller.capacitySnapshot().usedCapacity());
        h.controller.eventLog().dump();
    }

    /**
     * 等待者欠多份但释放的额度只够一部分：拿到能拿的，剩余部分继续排队，
     * 等下一次释放再补，直到收敛。
     */
    @Test
    void waiterOwingMultipleUnitsKeepsWaitingWhenReleaseIsPartial() {
        TestHarness h = new TestHarness(30_000L, 5);
        h.controller.registerWorkload("a", 3);
        h.controller.registerWorkload("b", 2);
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));
        h.controller.updatePolicy("b", minPolicy(2L, 5)); // b 欠 3 份
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());

        // a 缩 1（低负载一轮）：只腾 1 份 -> b 拿到 1 份到 3，仍欠 2 份继续排队
        h.controller.observeLoad(new LoadObservation("a", 1L, h.clock.nowMillis(), 0.1d));
        assertEquals(ScaleOutcome.SCALED_DOWN, h.controller.reconcileScale("a").outcome());
        assertEquals(2, h.controller.snapshot("a").instances().size());
        assertEquals(3, h.controller.snapshot("b").instances().size());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder(),
                "still-short waiter must stay queued, not silently drop its remaining demand");
        assertEquals(5, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[realloc] partial release: b=3 still owes 2, stays queued");

        // a 再缩 1 -> b 再补 1 份到 4，仍欠 1 份
        h.controller.observeLoad(new LoadObservation("a", 2L, h.clock.nowMillis(), 0.1d));
        h.controller.reconcileScale("a");
        assertEquals(1, h.controller.snapshot("a").instances().size());
        assertEquals(4, h.controller.snapshot("b").instances().size());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());

        // a 退出腾出最后 1 份 -> b 补到 5，队列清空，收敛
        h.controller.deregisterWorkload("a");
        assertEquals(5, h.controller.snapshot("b").instances().size());
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty());
        assertEquals(5, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[realloc] b converged to 5 across successive partial releases");
    }

    /**
     * 改策略腾额度：下调 a 的 maxReplicas 触发缩容，释放的额度必须在
     * updatePolicy 返回前当场分给等待者 b，不需要任何后续操作带出来。
     */
    @Test
    void capacityFreedByPolicyChangeIsReallocatedWithinSameOperation() {
        TestHarness h = new TestHarness(30_000L, 3);
        h.controller.registerWorkload("a", 2);
        h.controller.registerWorkload("b", 1); // 盘子满
        h.controller.updatePolicy("a", eagerPolicy(1L));
        h.controller.updatePolicy("b", eagerPolicy(1L));

        // b 想扩 -> 排队等额
        h.controller.observeLoad(new LoadObservation("b", 1L, h.clock.nowMillis(), 0.95d));
        assertEquals(ScaleOutcome.PENDING_CAPACITY, h.controller.reconcileScale("b").outcome());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingOrder());

        // 下调 a 的上限到 1：a 缩 2->1 腾出 1 份，b 必须当场拿到
        PolicyUpdateResult shrunk = h.controller.updatePolicy("a",
                new ScalePolicy(2L, 1, 1, 0.8d, 0.5d, 1, 0L,
                        false, 1, 2, 10_000L, 300L, 1_000L));
        assertTrue(shrunk.accepted());
        assertEquals(1, h.controller.snapshot("a").instances().size());
        assertEquals(2, h.controller.snapshot("b").instances().size(),
                "capacity freed by policy change must reach the waiter within the same operation");
        assertTrue(h.controller.capacitySnapshot().waitingOrder().isEmpty());
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[realloc] policy-driven scale-down fed waiter b in the same call");
        h.controller.eventLog().dump();
    }

    /**
     * 两个等待者各欠 2 份：额度分批释放时按 FIFO + 轮转分配（a 1 份 -> b 1 份 -> …），
     * 同一输入序列重跑两次，分配先后与最终收敛完全一致。
     */
    @Test
    void multipleWaitersOwingSeveralUnitsRotateAndConvergeDeterministically() {
        List<String> run1 = rotationScenario();
        List<String> run2 = rotationScenario();
        assertEquals(run1, run2, "rotation order and convergence must be identical across runs");
        assertEquals(List.of(
                "waiting:[a, b]",
                "a:3", "b:3", "waiting:[a, b]", "used:8",
                "a:4", "b:4", "waiting:[]", "used:8"), run1);
        System.out.println("[realloc-rotation] trace=" + run1);
    }

    /** 固定脚本：a、b 各欠 2 份，c、d 先后退出各腾 2 份，记录每个关键节点的状态。 */
    private List<String> rotationScenario() {
        TestHarness h = new TestHarness(30_000L, 8);
        List<String> trace = new ArrayList<>();
        for (String wl : List.of("a", "b", "c", "d")) {
            h.controller.registerWorkload(wl, 2); // 8 份占满
            h.controller.updatePolicy(wl, eagerPolicy(1L));
        }
        // a、b 先后抬 min 到 4：各欠 2 份，按先后入队
        h.controller.updatePolicy("a", minPolicy(2L, 4));
        h.controller.updatePolicy("b", minPolicy(2L, 4));
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());

        // c 退出腾 2 份 -> 轮转：a 拿 1 份、b 拿 1 份，二者仍各欠 1 份继续排队
        h.controller.deregisterWorkload("c");
        trace.add("a:" + h.controller.snapshot("a").instances().size());
        trace.add("b:" + h.controller.snapshot("b").instances().size());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());
        trace.add("used:" + h.controller.capacitySnapshot().usedCapacity());

        // d 退出再腾 2 份 -> a、b 各补 1 份到目标 4，队列清空，收敛
        h.controller.deregisterWorkload("d");
        trace.add("a:" + h.controller.snapshot("a").instances().size());
        trace.add("b:" + h.controller.snapshot("b").instances().size());
        trace.add("waiting:" + h.controller.capacitySnapshot().waitingOrder());
        trace.add("used:" + h.controller.capacitySnapshot().usedCapacity());
        return trace;
    }
}
