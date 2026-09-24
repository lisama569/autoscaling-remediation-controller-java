package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/**
 * 负载观测。version 单调递增；过期/回退观测必须被拒绝。
 */
public record LoadObservation(
        String workloadId,
        long version,
        long observedAtMillis,
        double loadRatio
) {
}
