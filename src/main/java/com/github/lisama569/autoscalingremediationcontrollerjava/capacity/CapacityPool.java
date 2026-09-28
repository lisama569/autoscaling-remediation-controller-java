package com.github.lisama569.autoscalingremediationcontrollerjava.capacity;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.CapacitySnapshot;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * 全局共享实例容量账本。
 *
 * <p>所有工作负载的实例都从同一个总盘子 {@code totalCapacity} 里占额度。本类用单一内部锁
 * 保护全部状态，自身线程安全；“占额度 + 创建/回收实例”这类跨账本与工作负载聚合的复合操作，
 * 由 {@code Controller} 按固定加锁顺序（先工作负载锁，后容量锁）串行化，避免死锁与超发。
 *
 * <p><b>分配规则（确定、可解释、无饿死）：</b>
 * <ul>
 *   <li>等待队列为 FIFO（{@link LinkedHashSet}：同一工作负载重复登记合并为一个条目，保持原排位）；</li>
 *   <li>{@link #tryAcquireFair} 一次只申请 1 份：只有“队列无前置等待者（自己是队首或队列为空）
 *       且盘子有空”才占用；成功后立即出队——还想要下一份的工作负载会在后续申请时重新排到队尾，
 *       因此多个工作负载轮流拿，谁也不会永远轮不到；</li>
 *   <li>额度不足或前方有等待者时返回 {@link CapacityAcquireStatus#QUEUED} 并在队尾登记（已在队列
 *       则保持原位），不占额度、不超发；</li>
 *   <li>{@link #releaseOne} / {@link #releaseAllAndRemove} 只更新账本，不在持锁时回调工作负载；
 *       派发由 {@code Controller} 在锁外按队列顺序重新 reconcile 完成，因此与线程调度无关。</li>
 * </ul>
 */
@Component
public class CapacityPool {

    private final int totalCapacity;

    /** workloadId -> 已占用额度；插入顺序即注册顺序，保证同序列重放迭代顺序一致。 */
    private final Map<String, Integer> allocations = new LinkedHashMap<>();

    /** FIFO 等待队列；同一工作负载至多一个条目。 */
    private final LinkedHashSet<String> waiters = new LinkedHashSet<>();

    public CapacityPool(@Value("${arc.total-capacity:20}") int totalCapacity) {
        if (totalCapacity < 1) {
            throw new IllegalArgumentException("totalCapacity must be >= 1, got " + totalCapacity);
        }
        this.totalCapacity = totalCapacity;
    }

    public synchronized int totalCapacity() {
        return totalCapacity;
    }

    /**
     * 注册时一次性原子占用 n 份初始额度。
     *
     * <p>严格 FIFO：只要队列里还有任何等待者，新注册不得插队；额度不足同样拒绝。
     * 任一条件不满足都抛 {@link CapacityUnavailableException} 且账本不变（不留半成品）。
     */
    public synchronized void reserveInitial(String workloadId, int replicas) {
        if (workloadId == null || workloadId.isBlank() || replicas < 1) {
            throw new IllegalArgumentException("workloadId required and replicas >= 1");
        }
        int used = usedLocked();
        if (!waiters.isEmpty()) {
            throw new CapacityUnavailableException(workloadId, replicas,
                    Math.max(0, totalCapacity - used), totalCapacity);
        }
        if (used + replicas > totalCapacity) {
            throw new CapacityUnavailableException(workloadId, replicas,
                    totalCapacity - used, totalCapacity);
        }
        allocations.merge(workloadId, replicas, Integer::sum);
    }

    /**
     * 公平申请 1 份额度。
     *
     * <p>仅当（队列为空或本工作负载在队首）且有空闲额度时占用并出队；
     * 否则登记到队尾（已登记则保持原位）返回 {@link CapacityAcquireStatus#QUEUED}。
     * 账本中不存在该工作负载（未注册或已注销）返回 {@link CapacityAcquireStatus#STALE}。
     */
    public synchronized CapacityAcquireStatus tryAcquireFair(String workloadId) {
        if (!allocations.containsKey(workloadId)) {
            return CapacityAcquireStatus.STALE;
        }
        boolean atHead = waiters.isEmpty() || waiters.iterator().next().equals(workloadId);
        if (atHead && usedLocked() + 1 <= totalCapacity) {
            allocations.merge(workloadId, 1, Integer::sum);
            waiters.remove(workloadId); // 消费完这一轮，出队；还想要下一份就重新排队（轮流）
            return CapacityAcquireStatus.ACQUIRED;
        }
        waiters.add(workloadId); // LinkedHashSet：已存在则保持原排位，新登记落到队尾
        return CapacityAcquireStatus.QUEUED;
    }

    /** 释放 1 份额度（缩容/实例回收）。不直接派发，仅更新账本。 */
    public synchronized void releaseOne(String workloadId) {
        Integer current = allocations.get(workloadId);
        if (current == null || current <= 0) {
            return;
        }
        if (current == 1) {
            allocations.put(workloadId, 0);
        } else {
            allocations.put(workloadId, current - 1);
        }
    }

    /** 工作负载整体退出：退还全部额度并移除其等待条目；返回实际释放的份数。 */
    public synchronized int releaseAllAndRemove(String workloadId) {
        Integer current = allocations.remove(workloadId);
        waiters.remove(workloadId);
        return current == null ? 0 : current;
    }

    /** 工作负载仍在队列中等待。 */
    public synchronized boolean isWaiting(String workloadId) {
        return waiters.contains(workloadId);
    }

    /**
     * 工作负载已无待满足的扩容需求，从等待队列移除自己的条目。
     * 只删自己的条目，不改变其余 FIFO 顺序。
     */
    public synchronized void removeWaiter(String workloadId) {
        waiters.remove(workloadId);
    }

    /** 当前等待队列快照（FIFO 顺序）。 */
    public synchronized List<String> waitingQueue() {
        return new ArrayList<>(waiters);
    }

    /** 不可变账本快照。 */
    public synchronized CapacitySnapshot snapshot() {
        int used = usedLocked();
        List<CapacitySnapshot.AllocationView> views = new ArrayList<>();
        for (Map.Entry<String, Integer> e : allocations.entrySet()) {
            views.add(new CapacitySnapshot.AllocationView(e.getKey(), e.getValue()));
        }
        return new CapacitySnapshot(totalCapacity, used, totalCapacity - used,
                views, new ArrayList<>(waiters));
    }

    private int usedLocked() {
        int sum = 0;
        for (int n : allocations.values()) {
            sum += n;
        }
        return sum;
    }
}
