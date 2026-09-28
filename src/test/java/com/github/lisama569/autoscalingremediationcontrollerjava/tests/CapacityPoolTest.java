package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityAcquireStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityPool;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityUnavailableException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 共享容量账本单元语义：初始占用、额度不足拒绝、FIFO 轮流分配、释放回收、退出清账，
 * 以及并发下总额恒不超上限。
 */
class CapacityPoolTest {

    @Test
    void reserveAndOverflow() {
        CapacityPool pool = new CapacityPool(5);
        pool.reserveInitial("a", 3);
        pool.reserveInitial("b", 2);
        assertEquals(5, pool.snapshot().usedCapacity());
        assertEquals(0, pool.snapshot().availableCapacity());
        assertThrows(CapacityUnavailableException.class, () -> pool.reserveInitial("c", 1));
        // 失败不留账
        assertEquals(2, pool.snapshot().allocations().size());
    }

    @Test
    void fifoFairAllocationAndTurnTaking() {
        CapacityPool pool = new CapacityPool(3);
        pool.reserveInitial("a", 1);
        pool.reserveInitial("b", 1);
        // 只剩 1 份：a 与 b 同时想要扩容
        assertEquals(CapacityAcquireStatus.ACQUIRED, pool.tryAcquireFair("a"));
        // 盘子满：b 排队
        assertEquals(CapacityAcquireStatus.QUEUED, pool.tryAcquireFair("b"));
        // a 再次想要也不能插队，且已登记过的 b 位置不变
        assertEquals(CapacityAcquireStatus.QUEUED, pool.tryAcquireFair("a"));
        assertEquals(List.of("b", "a"), pool.waitingQueue());

        // b 释放不出队；由调度器驱动：释放 1 份后，队首 b 拿到
        pool.releaseOne("a");
        assertEquals(CapacityAcquireStatus.ACQUIRED, pool.tryAcquireFair("b"));
        // b 拿到后出队；现在 a 是队首
        assertEquals(List.of("a"), pool.waitingQueue());
        // a 释放 1 份后，队首 a 拿到（等待者最终被满足、收敛）
        pool.releaseOne("b");
        assertEquals(CapacityAcquireStatus.ACQUIRED, pool.tryAcquireFair("a"));
        assertEquals(0, pool.waitingQueue().size());
    }

    @Test
    void releaseAndWorkloadExit() {
        CapacityPool pool = new CapacityPool(4);
        pool.reserveInitial("a", 2);
        pool.reserveInitial("b", 2);
        pool.releaseOne("a");
        assertEquals(3, pool.snapshot().usedCapacity());
        assertEquals(1, pool.snapshot().availableCapacity());
        int freed = pool.releaseAllAndRemove("b");
        assertEquals(2, freed);
        assertEquals(1, pool.snapshot().usedCapacity());
        assertEquals(3, pool.snapshot().availableCapacity());
        // 已注销工作负载申请无效
        assertEquals(CapacityAcquireStatus.STALE, pool.tryAcquireFair("b"));
    }

    @Test
    void concurrentContentionNeverOvercommits() throws Exception {
        CapacityPool p = new CapacityPool(10);
        for (int i = 0; i < 10; i++) {
            p.reserveInitial("w" + i, 1); // 放满总盘
        }
        // 10 个已注册工作负载并发申请（已无份额）：全部 QUEUED，used 恒为 10，绝不超发
        ExecutorService es = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger queued = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            String wl = "w" + i;
            es.submit(() -> {
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                if (p.tryAcquireFair(wl) == CapacityAcquireStatus.QUEUED) {
                    queued.incrementAndGet();
                }
            });
        }
        start.countDown();
        es.shutdown();
        assertTrue(es.awaitTermination(20, TimeUnit.SECONDS));
        assertEquals(10, queued.get());
        assertEquals(10, p.snapshot().usedCapacity());
        assertEquals(0, p.snapshot().availableCapacity());
        // 队列包含全部 10 个且无重复条目
        assertEquals(10, p.waitingQueue().size());
        assertEquals(10, new java.util.HashSet<>(p.waitingQueue()).size());
    }
}
