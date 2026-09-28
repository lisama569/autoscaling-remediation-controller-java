package com.github.lisama569.autoscalingremediationcontrollerjava.model;

import java.util.List;

/**
 * 全局共享实例容量的不可变快照（便于断言与解释）。
 *
 * @param totalCapacity 全局实例总盘子
 * @param usedCapacity  所有工作负载已占用的额度之和（恒有 used + available = total）
 * @param availableCapacity 当前可用额度
 * @param allocations   每个工作负载占用的额度（workloadId -> 占用数）
 * @param waitingQueue  等待额度的工作负载队列（FIFO，同一工作负载合并为一个条目）
 */
public record CapacitySnapshot(
        int totalCapacity,
        int usedCapacity,
        int availableCapacity,
        List<AllocationView> allocations,
        List<String> waitingQueue
) {
    public record AllocationView(String workloadId, int reserved) {
    }
}
