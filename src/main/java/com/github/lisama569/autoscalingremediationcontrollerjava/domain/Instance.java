package com.github.lisama569.autoscalingremediationcontrollerjava.domain;

/**
 * 进程内模拟的实例。
 */
public final class Instance {

    private final String id;
    private volatile InstanceStatus status;

    public Instance(String id, InstanceStatus status) {
        this.id = id;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public InstanceStatus getStatus() {
        return status;
    }

    public void setStatus(InstanceStatus status) {
        this.status = status;
    }
}
