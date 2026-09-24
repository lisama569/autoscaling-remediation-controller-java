package com.github.lisama569.autoscalingremediationcontrollerjava.domain;

/**
 * 一次负载/健康观测（不可变值对象）。
 * observedAt 由控制器时钟统一盖章，sequence 为来源端单调序号，用于识别过期/乱序观测。
 */
public record LoadObservation(
        String sourceId,
        long sequence,
        double observedLoad,
        boolean loadPresent,
        long observedAt
) {
}
