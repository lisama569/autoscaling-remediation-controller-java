package com.github.lisama569.autoscalingremediationcontrollerjava.capacity;

/**
 * 一次容量申请的结论（不可变）。
 *
 * @param granted        true=已从总盘占用 requestedUnits 份额度；false=额度不足，工作负载已入等待队列
 * @param requestedUnits 本次申请的额度份数
 * @param queuePosition  排队位置（队首为 1）；已授予时为 0
 * @param usedCapacity   申请处理后全集群已占用额度
 * @param totalCapacity  全局容量上限
 */
public record CapacityReservation(
        boolean granted,
        int requestedUnits,
        int queuePosition,
        int usedCapacity,
        int totalCapacity
) {
    public static CapacityReservation granted(int requestedUnits, int usedCapacity, int totalCapacity) {
        return new CapacityReservation(true, requestedUnits, 0, usedCapacity, totalCapacity);
    }

    public static CapacityReservation queued(int requestedUnits, int queuePosition,
                                            int usedCapacity, int totalCapacity) {
        return new CapacityReservation(false, requestedUnits, queuePosition, usedCapacity, totalCapacity);
    }
}
