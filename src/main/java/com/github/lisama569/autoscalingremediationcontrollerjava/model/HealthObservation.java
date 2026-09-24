package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/**
 * 健康观测。version 单调递增；过期/回退观测必须被拒绝。
 */
public record HealthObservation(
        String workloadId,
        String instanceId,
        long version,
        long observedAtMillis,
        boolean healthy
) {
}
