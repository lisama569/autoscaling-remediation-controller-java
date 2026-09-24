package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/** 一次伸缩评估的确定性结论。 */
public record ScaleDecision(
        String workloadId,
        ScaleOutcome outcome,
        int previousReplicas,
        int targetReplicas,
        long policyVersion,
        long observationVersion,
        RejectionReason rejectionReason,
        String rationale
) {
    public static ScaleDecision rejected(String workloadId, RejectionReason reason, String rationale,
                                         long policyVersion, long observationVersion) {
        return new ScaleDecision(workloadId, ScaleOutcome.REJECTED, -1, -1,
                policyVersion, observationVersion, reason, rationale);
    }
}
