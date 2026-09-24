package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/** 观测写入结论：版本回退/过期等必须可区分。 */
public record ObservationAcceptResult(
        boolean accepted,
        RejectionReason rejectionReason,
        long acceptedVersion,
        String rationale
) {
    public static ObservationAcceptResult ok(long version, String rationale) {
        return new ObservationAcceptResult(true, null, version, rationale);
    }

    public static ObservationAcceptResult reject(RejectionReason reason, long version, String rationale) {
        return new ObservationAcceptResult(false, reason, version, rationale);
    }
}
