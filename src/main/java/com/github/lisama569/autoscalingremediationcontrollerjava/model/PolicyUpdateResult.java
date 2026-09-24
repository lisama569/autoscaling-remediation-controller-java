package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/** 策略写入结论：接受或携带可区分拒绝原因。 */
public record PolicyUpdateResult(
        boolean accepted,
        RejectionReason rejectionReason,
        ScalePolicy appliedPolicy,
        String rationale
) {
    public static PolicyUpdateResult ok(ScalePolicy policy) {
        return new PolicyUpdateResult(true, null, policy, "policy v" + policy.version() + " applied");
    }

    public static PolicyUpdateResult reject(RejectionReason reason, String rationale) {
        return new PolicyUpdateResult(false, reason, null, rationale);
    }
}
