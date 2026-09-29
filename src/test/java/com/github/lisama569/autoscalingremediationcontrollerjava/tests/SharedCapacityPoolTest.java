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

    /** 已在队列中的工作负载重复申请不得改变其 FIFO 位置（旧实现会把它挪到队尾）。 */
    @Test
    void repeatedRequestFromWaiterDoesNotMoveItsQueuePosition() {
        SharedCapacityPool pool = new SharedCapacityPool(2);
        pool.requestScaleUpUnit("a");
        pool.requestScaleUpUnit("b"); // 满
        assertFalse(pool.requestScaleUpUnit("a").granted());
        assertFalse(pool.requestScaleUpUnit("b").granted()); // [a, b]
        // a 重复申请（例如重复评估）：位置仍是 1，不能把自己挪到 b 后面，也不影响 b 的位置
        CapacityReservation repeat = pool.requestScaleUpUnit("a");
        assertFalse(repeat.granted());
        assertEquals(1, repeat.queuePosition());
        assertEquals(List.of("a", "b"), pool.snapshot().waitingOrder());

        // drain 专用授予：队首 a 拿 1 份并要求轮转 -> 回到队尾，b 成为新队首
        pool.release("b", 1);
        CapacityReservation granted = pool.grantQueuedUnit("a", true);
        assertTrue(granted.granted());
        assertEquals(List.of("b", "a"), pool.snapshot().waitingOrder(),
                "granted waiter with remaining demand rotates to the tail");
        // 最后一份需求：不轮转，直接离队
        pool.release("a", 1);
        // b 先拿（队首），离队；再释放让 a 也能拿
        CapacityReservation forB = pool.grantQueuedUnit("b", false);
        assertTrue(forB.granted());
        assertEquals(List.of("a"), pool.snapshot().waitingOrder());
        System.out.println("[pool] repeat request preserves FIFO position; grant rotation works");
    }

    /** 非队首工作负载即便总盘有空余，也不能通过 reserveOrQueue 插队拿额度。 */
    @Test
    void reserveOrQueueRespectsHeadOfLineAndQueuedUnitGrantStopsWhenPoolEmpty() {
        SharedCapacityPool pool = new SharedCapacityPool(3);
        pool.reserveInitial("holder", 3); // 占满
        assertFalse(pool.requestScaleUpUnit("a").granted()); // 队首等待者 a
        pool.release("holder", 1); // 空余 1 份，队首仍是 a
        // b 后来者：即使有空余也不能一次性预留，必须排在 a 后面
        CapacityReservation b = pool.reserveOrQueue("b", 1);
        assertFalse(b.granted());
        assertEquals(2, b.queuePosition());
        assertEquals(List.of("a", "b"), pool.snapshot().waitingOrder());

        // drain：a 拿走唯一空余（轮转回队尾，因为测试让它仍有需求）
        CapacityReservation g = pool.grantQueuedUnit("a", true);
        assertTrue(g.granted());
        assertEquals(3, pool.usedCapacity());
        // 盘子再空时，队首 b 的授予尝试必须不授予且保持队首位置
        CapacityReservation none = pool.grantQueuedUnit("b", false);
        assertFalse(none.granted());
        assertEquals(List.of("b", "a"), pool.snapshot().waitingOrder());
        assertEquals("b", pool.headWaiter(), "b keeps head-of-line position while pool is empty");
        System.out.println("[pool] non-head cannot jump queue; empty-pool grant keeps head waiting");
    }
}
