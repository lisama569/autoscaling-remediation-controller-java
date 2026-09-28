package com.github.lisama569.autoscalingremediationcontrollerjava.capacity;

/**
 * 注册工作负载时，初始副本无法从全局容量总盘子中占用（额度不足或前方有等待者）。
 *
 * <p>与伸缩场景的 {@code CAPACITY_PENDING} 结论不同：注册是一次性的原子动作，
 * 初始副本必须立即全部落地，额度不够则明确失败，不留半成品。
 */
public class CapacityUnavailableException extends RuntimeException {

    private final int requested;
    private final int available;
    private final int totalCapacity;

    public CapacityUnavailableException(String workloadId, int requested, int available, int totalCapacity) {
        super("global capacity unavailable for workload " + workloadId
                + ": requested " + requested + ", available " + available + ", total " + totalCapacity);
        this.requested = requested;
        this.available = available;
        this.totalCapacity = totalCapacity;
    }

    public int requested() {
        return requested;
    }

    public int available() {
        return available;
    }

    public int totalCapacity() {
        return totalCapacity;
    }
}
