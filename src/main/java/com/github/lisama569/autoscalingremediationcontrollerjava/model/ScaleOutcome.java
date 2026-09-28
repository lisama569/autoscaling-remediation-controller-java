package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/**
 * 一次伸缩评估的结果类别。IN_COOLDOWN 与 REJECTED 区分：
 * 前者输入合法但处于冷却窗口，后者是前置条件不满足（非法/信息不足/冲突）。
 */
public enum ScaleOutcome {
    SCALED_UP,
    SCALED_DOWN,
    NO_CHANGE,
    IN_COOLDOWN,
    /** 扩容意向合法（区别于策略非法/观测缺失或过期/冷却中），但全局共享容量不足，已进入 FIFO 等待队列。 */
    CAPACITY_PENDING,
    REJECTED
}
