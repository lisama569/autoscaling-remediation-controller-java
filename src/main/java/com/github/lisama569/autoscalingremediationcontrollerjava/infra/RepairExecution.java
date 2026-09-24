package com.github.lisama569.autoscalingremediationcontrollerjava.infra;

/** 一次模拟修复执行的确定性结果。 */
public record RepairExecution(
        boolean success,
        long durationMillis,
        String replacedInstanceId,
        String detail
) {
}
