package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/** 请求修复一个实例的结论（与任务最终执行状态区分）。 */
public record RemediationRequestOutcome(
        String workloadId,
        String instanceId,
        RemediationRequestResult result,
        String taskId,
        RejectionReason rejectionReason,
        String rationale
) {
}
