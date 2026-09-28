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
    /** 伸缩意向合法且已通过稳定/冷却/去重判定，但共享容量不足，已进入等待队列。 */
    PENDING_CAPACITY,
    REJECTED
}
