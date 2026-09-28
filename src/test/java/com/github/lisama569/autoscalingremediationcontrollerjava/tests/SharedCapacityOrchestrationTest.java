package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityUnavailableException;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多工作负载共享容量协同（第一部分：占满→等待→释放→补上；初始额度不足；退出回收）。
 */
class SharedCapacityOrchestrationTest {

    /** rounds=1, cooldown=0, min=1, max 很大；高负载即每轮扩 1，低负载即每轮缩 1。 */
    static ScalePolicy eagerPolicy(long version) {
        return new ScalePolicy(version, 1, 100, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
    }

    static void load(TestHarness h, String wl, long v, double ratio) {
        h.controller.observeLoad(new LoadObservation(wl, v, h.clock.nowMillis(), ratio));
    }

    @Test
    void oneWorkloadFillsPoolAnotherWaitsThenGetsCapacityAfterRelease() {
        TestHarness h = new TestHarness(30_000L, 3);
        // a 注册 2 份，占满 2/3
        assertTrue(h.controller.registerWorkload("a", 2));
        // b 只需 1 份：可以注册（剩 1）
        assertTrue(h.controller.registerWorkload("b", 1));
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());

        h.controller.updatePolicy("a", eagerPolicy(1));
        h.controller.updatePolicy("b", eagerPolicy(1));

        // a 想扩容：盘子满 -> 明确的 CAPACITY_PENDING（区别于冷却/非法/观测问题）
        load(h, "a", 1L, 0.95);
        ScaleDecision aBlocked = h.controller.reconcileScale("a");
        assertEquals(ScaleOutcome.CAPACITY_PENDING, aBlocked.outcome());
        assertEquals(RejectionReason.INSUFFICIENT_GLOBAL_CAPACITY, aBlocked.rejectionReason());
        assertEquals(2, h.controller.snapshot("a").instances().size());
        System.out.println("[capacity] a blocked: " + aBlocked.rationale());

        // b 也想扩容：a 已排队，b 不能插队
        load(h, "b", 1L, 0.95);
        ScaleDecision bBlocked = h.controller.reconcileScale("b");
        assertEquals(ScaleOutcome.CAPACITY_PENDING, bBlocked.outcome());
        assertEquals(List.of("a", "b"), h.controller.capacitySnapshot().waitingQueue());

        // a 缩容释放 1 份：2->1。释放事件立即在锁外驱动 FIFO 调度：
        // 队首 a 重评（最新是低负载 v2，已在地板 min=1）不再需要额度 -> 出队；
        // 队首 b 重评其尚未消费的高负载 v1 -> 拿到刚释放的 1 份，1->2。
        load(h, "a", 2L, 0.2);
        ScaleDecision aDown = h.controller.reconcileScale("a");
        assertEquals(ScaleOutcome.SCALED_DOWN, aDown.outcome());

        // 释放即收敛：b 已在同一次事件后的调度中补到额度，无需再手动调度
        assertEquals(2, h.controller.snapshot("b").instances().size());
        assertEquals(0, h.controller.capacitySnapshot().waitingQueue().size());
        assertEquals(3, h.controller.capacitySnapshot().usedCapacity());
        // 再调一次调度也是幂等空转
        assertEquals(0, h.controller.schedulePendingAllocations());
        System.out.println("[capacity] after release a=1 b=2, queue empty, used=3");
        h.controller.eventLog().dump();
    }

    @Test
    void initialRegistrationRejectedWhenPoolExhausted() {
        TestHarness h = new TestHarness(30_000L, 3);
        assertTrue(h.controller.registerWorkload("a", 2));
        CapacityUnavailableException ex = assertThrows(CapacityUnavailableException.class,
                () -> h.controller.registerWorkload("c", 2));
        assertEquals(2, ex.requested());
        assertEquals(1, ex.available());
        // 失败不留注册、不留账
        assertEquals(null, h.controller.snapshot("c"));
        assertEquals(1, h.controller.capacitySnapshot().allocations().size());
        System.out.println("[capacity] initial registration rejected: " + ex.getMessage());
    }

    @Test
    void unregisterFreesAllCapacityAndDrainsWaiters() {
        TestHarness h = new TestHarness(30_000L, 2);
        assertTrue(h.controller.registerWorkload("a", 1));
        assertTrue(h.controller.registerWorkload("b", 1));
        h.controller.updatePolicy("a", eagerPolicy(1));
        h.controller.updatePolicy("b", eagerPolicy(1));
        load(h, "a", 1L, 0.95);
        assertEquals(ScaleOutcome.CAPACITY_PENDING, h.controller.reconcileScale("a").outcome());
        load(h, "b", 1L, 0.95);
        assertEquals(ScaleOutcome.CAPACITY_PENDING, h.controller.reconcileScale("b").outcome());

        // a 退出：释放 1 份；锁外调度，队首原本是 a（已注销被清），b 拿到额度 1->2
        assertTrue(h.controller.unregisterWorkload("a"));
        assertEquals(2, h.controller.snapshot("b").instances().size());
        assertEquals(0, h.controller.capacitySnapshot().waitingQueue().size());
        assertEquals(2, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[capacity] unregister a drained waiter b -> 2");
    }

    /** 简单的确定性辅助：记录各工作负载最终实例数与队列。 */
    static List<Integer> finals(TestHarness h, List<String> ids) {
        List<Integer> out = new ArrayList<>();
        for (String id : ids) {
            WorkloadState s = h.controller.snapshot(id);
            out.add(s == null ? -1 : s.instances().size());
        }
        return out;
    }

    @Test
    void policyMinBumpIsThrottledByCapacityAndConvergesAfterRelease() {
        TestHarness h = new TestHarness(30_000L, 2);
        h.controller.registerWorkload("a", 1);
        h.controller.registerWorkload("b", 1); // 占满 2
        // 无负载观测也允许策略钳制：把 a 的 min 提到 3，欠账 2 份；盘子满 -> 只能排队，实例仍 1
        ScalePolicy a3 = new ScalePolicy(2L, 3, 10, 0.8d, 0.5d, 1, 0L,
                false, 1, 2, 10_000L, 300L, 1_000L);
        h.controller.updatePolicy("a", a3);
        assertEquals(1, h.controller.snapshot("a").instances().size());
        assertTrue(h.controller.capacitySnapshot().waitingQueue().contains("a"));
        System.out.println("[capacity] min-bump queued: " + h.controller.capacitySnapshot().waitingQueue());

        // b 退出释放 1 份：a 立刻补上 1 份（1->2），仍欠 1 份留在队中（共需 3、总盘 2）
        h.controller.unregisterWorkload("b");
        assertEquals(2, h.controller.snapshot("a").instances().size());
        assertEquals(1, h.controller.capacitySnapshot().waitingQueue().size());
        // 总盘只有 2，a 永远只能到 2：账本与实例一致，不超发
        assertEquals(2, h.controller.capacitySnapshot().usedCapacity());
        System.out.println("[capacity] min-bump partially granted a=2, still waiting for the 3rd");

        // 无额度可再分时调度幂等空转，返回 0
        assertEquals(0, h.controller.schedulePendingAllocations());
    }
}
