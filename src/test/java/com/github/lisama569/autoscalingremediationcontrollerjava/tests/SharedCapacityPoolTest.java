package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityReservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.SharedCapacityPool;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 共享容量池账本与 FIFO 规则的单元测试：
 * 额度占用/超发、队首优先不插队、释放回收、退出归还、无饥饿轮转。
 */
class SharedCapacityPoolTest {

    @Test
    void reserveAndExhaustReturnsDistinguishableQueuedResult() {
        SharedCapacityPool pool = new SharedCapacityPool(3);
        CapacityReservation r1 = pool.reserveInitial("a", 2);
        assertTrue(r1.granted());
        assertEquals(2, pool.usedCapacity());

        CapacityReservation r2 = pool.reserveInitial("b", 2); // 只剩 1 份
        assertFalse(r2.granted());
        // 初始注册容量不足不排队、一份不占（调用方整体拒绝）
        assertEquals(2, pool.usedCapacity());
        assertTrue(pool.snapshot().waitingOrder().isEmpty());
        System.out.println("[pool] initial reserve exhausted -> not granted, used stays " + pool.usedCapacity());
    }

    @Test
    void fifoHeadOfLineDoesNotLetLaterRequestsJumpQueue() {
        SharedCapacityPool pool = new SharedCapacityPool(2);
        assertTrue(pool.requestScaleUpUnit("a").granted());
        assertTrue(pool.requestScaleUpUnit("b").granted()); // 盘子满

        CapacityReservation a2 = pool.requestScaleUpUnit("a"); // a 还想扩，排队首位
        assertFalse(a2.granted());
        assertEquals(1, a2.queuePosition());

        CapacityReservation c1 = pool.requestScaleUpUnit("c"); // 后来者
        assertFalse(c1.granted());
        assertEquals(2, c1.queuePosition());

        // 即使 a 恰好是队首但当前无空闲，c 也不能插队
        assertEquals(List.of("a", "c"), pool.snapshot().waitingOrder());

        pool.release("a", 1); // 释放 1 份
        // 队首 a 拿到（c 不能抢）
        CapacityReservation aRetry = pool.requestScaleUpUnit("a");
        assertTrue(aRetry.granted());
        assertFalse(pool.isWaiting("a"));
        assertTrue(pool.isWaiting("c"));
        System.out.println("[pool] FIFO head-of-line respected: a granted before c");
    }

    @Test
    void releaseReturnsCapacityAndDeregisterFreesEverything() {
        SharedCapacityPool pool = new SharedCapacityPool(4);
        pool.reserveInitial("a", 3);
        CapacityReservation b = pool.reserveOrQueue("b", 2); // 只剩 1，不足 2 -> 排队
        assertFalse(b.granted());
        assertEquals(List.of("b"), pool.snapshot().waitingOrder());

        pool.release("a", 1); // a 缩 1，空闲变成 2
        // 但释放不自动授予；由上层 drain 按申请规则来
        assertEquals(2, pool.usedCapacity());
        assertEquals(2, pool.availableCapacity());

        // b 整体退出等待之外的场景：deregister 归还全部 a 额度
        pool.releaseAllAndRemove("b");
        assertFalse(pool.isWaiting("b"));
        pool.releaseAllAndRemove("a");
        assertEquals(0, pool.usedCapacity());
        assertEquals(4, pool.availableCapacity());
        System.out.println("[pool] release/deregister accounting correct");
    }

    @Test
    void headOfLineGrantOrderIsDeterministic() {
        SharedCapacityPool pool = new SharedCapacityPool(2);
        pool.requestScaleUpUnit("a");
        pool.requestScaleUpUnit("b"); // 满
        pool.requestScaleUpUnit("a"); // 等待队列: a
        pool.requestScaleUpUnit("b"); // 等待队列: a,b

        // 释放 2 份后，每次只授予队首：先 a（拿到后离开队列），再 b。顺序完全确定。
        pool.release("a", 1);
        pool.release("b", 1);
        List<String> grantOrder = new java.util.ArrayList<>();
        for (int i = 0; i < 2; i++) {
            String head = pool.headWaiter();
            CapacityReservation g = pool.requestScaleUpUnit(head);
            if (g.granted()) {
                grantOrder.add(head);
            }
        }
        assertEquals(List.of("a", "b"), grantOrder);
        assertTrue(pool.snapshot().waitingOrder().isEmpty());
        System.out.println("[pool] deterministic head-of-line grant order: " + grantOrder);
    }
}
