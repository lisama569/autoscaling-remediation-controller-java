package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 共享容量与既有规则的配合：
 *  - 冷却判定优先于容量：冷却中先给 IN_COOLDOWN（不入容量队列）；冷却结束才尝试占额；
 *  - 容量不足给独立的 CAPACITY_PENDING；
 *  - 冷却结束后等待者由 FIFO 调度补上，顺序严格可解释。
 */
class SharedCapacityCooperationTest {

    static ScalePolicy policy(long version, long cooldown) {
        return new ScalePolicy(version, 1, 100, 0.8d, 0.5d, 1, cooldown,
                false, 1, 2, 10_000L, 300L, 1_000L);
    }

    static void load(TestHarness h, String wl, long v, double ratio) {
        h.controller.observeLoad(new LoadObservation(wl, v, h.clock.nowMillis(), ratio));
    }

    @Test
    void fifoOrderIsStrictAndHeadExitDrainsNextWaiter() {
        TestHarness h = new TestHarness(30_000L, 2);
        h.controller.registerWorkload("a", 1);
        h.controller.registerWorkload("b", 1); // 占满 2
        h.controller.updatePolicy("a", policy(1, 0L));
        h.controller.updatePolicy("b", policy(1, 0L));

        // b 先想扩容：盘子满 -> CAPACITY_PENDING，b 在队首
        load(h, "b", 1L, 0.95);
        ScaleDecision bWait = h.controller.reconcileScale("b");
        assertEquals(ScaleOutcome.CAPACITY_PENDING, bWait.outcome());
        assertEquals(List.of("b"), h.controller.capacitySnapshot().waitingQueue());

        // a 后想扩容：不能插到 b 前面，排到队尾
        load(h, "a", 1L, 0.95);
        ScaleDecision aWait = h.controller.reconcileScale("a");
        assertEquals(ScaleOutcome.CAPACITY_PENDING, aWait.outcome());
        assertEquals(List.of("b", "a"), h.controller.capacitySnapshot().waitingQueue());
        System.out.println("[coop] FIFO before release: " + h.controller.capacitySnapshot().waitingQueue());

        // 队首 b 退出：释放 1 份并清自己的条目 -> 队首变 a，a 拿到额度 1->2
        h.controller.unregisterWorkload("b");
        assertEquals(2, h.controller.snapshot("a").instances().size());
        assertEquals(0, h.controller.capacitySnapshot().waitingQueue().size());
        System.out.println("[coop] b exited, a granted -> "
                + h.controller.snapshot("a").instances().size());
    }

    @Test
    void cooldownPrecedesCapacityGateAndConvergesAfterCooldownEnds() {
        TestHarness h = new TestHarness(30_000L, 2);
        h.controller.registerWorkload("a", 2); // a 独占 2 份（总盘满）
        h.controller.updatePolicy("a", policy(1, 500L));

        // 先成功扩一次容：需要空额度，所以另选一个初始有空闲的场景——直接构造容量 3
        TestHarness g = new TestHarness(30_000L, 3);
        g.controller.registerWorkload("a", 2);
        g.controller.updatePolicy("a", policy(1, 500L));
        load(g, "a", 1L, 0.95);
        assertEquals(ScaleOutcome.SCALED_UP, g.controller.reconcileScale("a").outcome()); // 2->3 满盘

        // 立即再来高负载：此时容量满 且 冷却中。判定顺序：冷却在容量闸门之前 -> IN_COOLDOWN
        load(g, "a", 2L, 0.95);
        ScaleDecision cooling = g.controller.reconcileScale("a");
        assertEquals(ScaleOutcome.IN_COOLDOWN, cooling.outcome());
        // 冷却期间不得占用额度、也不得登记容量等待（还没走到闸门）
        assertEquals(3, g.controller.capacitySnapshot().usedCapacity());
        assertEquals(0, g.controller.capacitySnapshot().waitingQueue().size());
        System.out.println("[coop] cooldown takes precedence: " + cooling.rationale());

        // 冷却结束但盘子仍满：这次越过冷却，走到容量闸门 -> CAPACITY_PENDING
        g.advance(501L);
        load(g, "a", 3L, 0.95);
        ScaleDecision pending = g.controller.reconcileScale("a");
        assertEquals(ScaleOutcome.CAPACITY_PENDING, pending.outcome());
        assertEquals(1, g.controller.capacitySnapshot().waitingQueue().size());
        System.out.println("[coop] after cooldown, now capacity-pending: " + pending.rationale());

        // 释放 1 份后（a 低负载缩到 2），队列清理；结论链路完整可复现
        load(g, "a", 4L, 0.2);
        assertEquals(ScaleOutcome.SCALED_DOWN, g.controller.reconcileScale("a").outcome());
        assertEquals(2, g.controller.capacitySnapshot().usedCapacity());
        g.controller.eventLog().dump();
    }
}
