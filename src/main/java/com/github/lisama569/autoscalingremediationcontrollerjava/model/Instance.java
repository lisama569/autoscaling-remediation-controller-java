package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/** 进程内模拟实例（可变状态）。 */
public final class Instance {
    private final String id;
    private final String workloadId;
    private volatile InstanceStatus status;
    private final long createdAtMillis;

    public Instance(String id, String workloadId, InstanceStatus status, long createdAtMillis) {
        this.id = id;
        this.workloadId = workloadId;
        this.status = status;
        this.createdAtMillis = createdAtMillis;
    }

    public String getId() { return id; }
    public String getWorkloadId() { return workloadId; }
    public InstanceStatus getStatus() { return status; }
    public void setStatus(InstanceStatus status) { this.status = status; }
    public long getCreatedAtMillis() { return createdAtMillis; }
}
