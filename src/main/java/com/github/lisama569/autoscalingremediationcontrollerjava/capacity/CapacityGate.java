package com.github.lisama569.autoscalingremediationcontrollerjava.capacity;

/**
 * 伸缩评估向共享容量池申请额度的网关（由 {@link SharedCapacityPool} 实现）。
 *
 * <p>方法在调用方持有工作负载串行锁时执行；实现内部使用独立的全局锁，
 * 锁顺序固定为「工作负载锁 → 容量锁」，杜绝锁顺序环。
 */
public interface CapacityGate {

    /**
     * 为一次扩容（1 个副本）申请额度。
     *
     * @param workloadId 发起申请的工作负载
     * @return 已授予（立即占用 1 份）或已排队（FIFO，返回队列位置）
     */
    CapacityReservation requestScaleUpUnit(String workloadId);

    /** 取消该工作负载的等待（意向撤销/转为缩容时），不在队列中则为空操作。 */
    void cancelWait(String workloadId);
}
