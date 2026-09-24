package com.github.lisama569.autoscalingremediationcontrollerjava.domain;

/**
 * 实例生命周期状态。
 */
public enum InstanceStatus {
    HEALTHY,
    UNHEALTHY,
    REPAIRING,
    REPLACED,
    REMEDIATION_FAILED,
    TERMINATED
}
