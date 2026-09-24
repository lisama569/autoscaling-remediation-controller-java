package com.github.lisama569.autoscalingremediationcontrollerjava.model;

import java.util.List;
import java.util.Optional;

/** 单个工作负载的完整聚合状态快照（不可变，便于断言与解释）。 */
public record WorkloadState(
        String workloadId,
        ScalePolicy policy,
        long latestLoadVersion,
        LoadObservation latestLoad,
        int desiredReplicas,
        long lastScaleAtMillis,
        int upStreak,
        int downStreak,
        List<Instance> instances,
        List<RemediationTask> remediations
) {
    public Optional<ScalePolicy> policyOpt() { return Optional.ofNullable(policy); }
}
