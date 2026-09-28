package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/** 工作负载注册结论（不可变）。 */
public record RegistrationResult(
        String workloadId,
        boolean registered,
        int grantedReplicas,
        RejectionReason rejectionReason,
        String rationale
) {
    public static RegistrationResult ok(String workloadId, int replicas) {
        return new RegistrationResult(workloadId, true, replicas, null,
                "registered with " + replicas + " initial replicas");
    }

    public static RegistrationResult alreadyExists(String workloadId, int replicas) {
        return new RegistrationResult(workloadId, false, replicas, null,
                "workload already registered; state untouched");
    }

    public static RegistrationResult rejected(String workloadId, RejectionReason reason, String rationale) {
        return new RegistrationResult(workloadId, false, 0, reason, rationale);
    }
}
