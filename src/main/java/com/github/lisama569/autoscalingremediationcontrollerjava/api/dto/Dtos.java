package com.github.lisama569.autoscalingremediationcontrollerjava.api.dto;

import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;

/**
 * REST 层请求/视图 DTO 集合。
 */
public final class Dtos {

    private Dtos() {
    }

    public record RegisterRequest(String workloadId, ScalingPolicy policy, Integer initialReplicas) {
    }

    public record UpdatePolicyRequest(ScalingPolicy policy) {
    }

    public record ObserveRequest(String sourceId, Long sequence, Double load) {
    }

    public record HealthRequest(String instanceId, Boolean healthy) {
    }

    public record TickRequest(Long deltaMillis) {
    }

    public record RemediateRequest(String instanceId) {
    }
}
