package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/** 进程内模拟的实例生命周期状态。 */
public enum InstanceStatus {
    PROVISIONING,
    READY,
    UNHEALTHY,
    REPAIRING,
    TERMINATED
}
