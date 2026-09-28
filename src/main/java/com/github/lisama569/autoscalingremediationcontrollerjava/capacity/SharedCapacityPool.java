package com.github.lisama569.autoscalingremediationcontrollerjava.capacity;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * 多工作负载共享的全局实例容量池。
 *
 * <p>所有工作负载的实例都从同一个总盘（{@code totalCapacity}）里占额度，
 * 因此不变量「Σ 各工作负载活动实例数 ≤ totalCapacity」由本组件的原子记账保证，
 * 与线程交错无关。
 *
 * <p><b>分配规则（确定、可解释、无饥饿）：</b>
 * <ul>
 *   <li>额度充足时立即占用；额度不足时申请进入 <b>FIFO 等待队列</b>（队首优先）。</li>
 *   <li>只要队首在等待，空闲额度只留给队首——后来者不能插队抢额度，
 *       因此任何一个工作负载都不会被无限期饿死。</li>
 *   <li>额度释放本身不直接创建实例；由 {@code Controller.reconcilePendingCapacity()}
 *       在后续事件/调度中按队列顺序逐份补分配（同一次重分配中每个工作负载一次只取 1 份，
 *       然后回到队尾，形成轮转），最终各自收敛。</li>
 * </ul>
 *
 * <p><b>并发：</b>所有记账在内部 {@code this} 锁下原子完成。上层 Controller 固定按
 * 「工作负载锁 → 容量锁」的顺序加锁，不存在反向获取，故无锁顺序环。
 */
@Component
public class SharedCapacityPool implements CapacityGate {

    private final int totalCapacity;

    /** 已占用额度（等于所有工作负载活动实例数之和）。 */
    private int used;

    /** 每个工作负载已授予的额度（等于其 desired 落地目标；活动实例数最终收敛到它）。 */
    private final Map<String, Integer> allocations = new HashMap<>();

    /** 等待额度的工作负载，FIFO 且去重；同一工作负载至多占一个队列位置。 */
    private final LinkedHashSet<String> waiters = new LinkedHashSet<>();

    public SharedCapacityPool(@Value("${arc.total-capacity:100}") int totalCapacity) {
        if (totalCapacity < 1) {
            throw new IllegalArgumentException("totalCapacity must be >= 1, got " + totalCapacity);
        }
        this.totalCapacity = totalCapacity;
    }

    public int totalCapacity() {
        return totalCapacity;
    }

    public synchronized int usedCapacity() {
        return used;
    }

    public synchronized int availableCapacity() {
        return totalCapacity - used;
    }

    /** 该工作负载已被授予、必须最终落地的额度份数。 */
    public synchronized int allocationOf(String workloadId) {
        return allocations.getOrDefault(workloadId, 0);
    }

    public synchronized CapacitySnapshot snapshot() {
        return new CapacitySnapshot(totalCapacity, used, new ArrayList<>(waiters));
    }

    /**
     * 注册工作负载时一次性预留 {@code initialUnits} 份初始额度。
     * 与运行期伸缩不同：容量不足时 <b>不排队</b>（工作负载尚未建立，没有可保留的意向），
     * 一份不占并返回 queued 结论（{@code queuePosition=0}），由调用方整体拒绝注册。
     */
    public synchronized CapacityReservation reserveInitial(String workloadId, int initialUnits) {
        if (initialUnits < 0) {
            throw new IllegalArgumentException("initialUnits must be >= 0, got " + initialUnits);
        }
        if (used + initialUnits <= totalCapacity) {
            used += initialUnits;
            allocations.merge(workloadId, initialUnits, Integer::sum);
            return CapacityReservation.granted(initialUnits, used, totalCapacity);
        }
        return CapacityReservation.queued(initialUnits, 0, used, totalCapacity);
    }

    /**
     * 预留 {@code units} 份额度（策略抬高 minReplicas 等场景）。
     * 能全部满足才占用；否则一份不占、工作负载加入队尾等待后续补分配。
     */
    public synchronized CapacityReservation reserveOrQueue(String workloadId, int units) {
        if (units <= 0) {
            return CapacityReservation.granted(0, used, totalCapacity);
        }
        if (used + units <= totalCapacity) {
            used += units;
            allocations.merge(workloadId, units, Integer::sum);
            return CapacityReservation.granted(units, used, totalCapacity);
        }
        enqueue(workloadId);
        return CapacityReservation.queued(units, positionOf(workloadId), used, totalCapacity);
    }

    /**
     * 为一次扩容（1 个副本）申请额度：满足立即占用 1 份；否则加入队尾。
     * 已在队列中的工作负载重复申请直接返回其当前排队位置，不重复入队。
     */
    @Override
    public synchronized CapacityReservation requestScaleUpUnit(String workloadId) {
        // 队首优先 + 容量充足才授予；队首不是自己时即使有空余也不抢（不插队）
        if ((waiters.isEmpty() || waiters.iterator().next().equals(workloadId))
                && used + 1 <= totalCapacity) {
            waiters.remove(workloadId);
            used += 1;
            allocations.merge(workloadId, 1, Integer::sum);
            return CapacityReservation.granted(1, used, totalCapacity);
        }
        enqueue(workloadId);
        return CapacityReservation.queued(1, positionOf(workloadId), used, totalCapacity);
    }

    /**
     * 释放 {@code units} 份额度（缩容/实例回收）。释放本身不触发补分配；
     * 由调用方随后按 FIFO 执行 {@code Controller.reconcilePendingCapacity()}。
     */
    public synchronized void release(String workloadId, int units) {
        if (units < 0) {
            throw new IllegalArgumentException("units must be >= 0, got " + units);
        }
        if (units == 0) {
            return;
        }
        int current = allocations.getOrDefault(workloadId, 0);
        int actual = Math.min(units, current);
        if (actual > 0) {
            allocations.put(workloadId, current - actual);
            used -= actual;
        }
    }

    /** 工作负载整体退出：归还其全部已授予额度并移出等待队列。 */
    public synchronized void releaseAllAndRemove(String workloadId) {
        Integer held = allocations.remove(workloadId);
        if (held != null) {
            used -= held;
        }
        waiters.remove(workloadId);
    }

    /** 取消该工作负载的等待（缩容到无需扩容、意向撤销时）。 */
    public synchronized void cancelWait(String workloadId) {
        waiters.remove(workloadId);
    }

    /** 队首工作负载 id（无等待返回 null）。 */
    public synchronized String headWaiter() {
        return waiters.isEmpty() ? null : waiters.iterator().next();
    }

    public synchronized boolean isWaiting(String workloadId) {
        return waiters.contains(workloadId);
    }

    /** 等待队列长度。 */
    public synchronized int waitingCount() {
        return waiters.size();
    }

    private void enqueue(String workloadId) {
        waiters.remove(workloadId);
        waiters.add(workloadId);
    }

    private int positionOf(String workloadId) {
        int pos = 1;
        for (String w : waiters) {
            if (w.equals(workloadId)) {
                return pos;
            }
            pos++;
        }
        return 0;
    }
}
