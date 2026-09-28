package com.github.lisama569.autoscalingremediationcontrollerjava.capacity;

/**
 * 单次（1 份）公平额度申请的结果。
 * <ul>
 *   <li>{@link #ACQUIRED}：额度已占用，调用方可以立刻创建实例；</li>
 *   <li>{@link #QUEUED}：额度不足或 FIFO 队列前方有其他等待者，已在队尾登记（已在队列则原地等待）；</li>
 *   <li>{@link #STALE}：该工作负载已退出/不再占用账本（例如已注销），申请无效。</li>
 * </ul>
 */
public enum CapacityAcquireStatus {
    ACQUIRED,
    QUEUED,
    STALE
}
