package com.github.lisama569.autoscalingremediationcontrollerjava.capacity;

import java.util.List;

/**
 * 共享容量池的不可变快照（用于断言、日志与解释）。
 *
 * @param totalCapacity 全局容量上限
 * @param usedCapacity  已占用额度（等于所有工作负载活动实例数之和）
 * @param waitingOrder  等待队列中的工作负载 id，按 FIFO 顺序（队首最先被分配）
 */
public record CapacitySnapshot(
        int totalCapacity,
        int usedCapacity,
        List<String> waitingOrder
) {
    public int availableCapacity() {
        return totalCapacity - usedCapacity;
    }
}
