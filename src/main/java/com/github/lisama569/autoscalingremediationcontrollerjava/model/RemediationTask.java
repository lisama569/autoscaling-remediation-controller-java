package com.github.lisama569.autoscalingremediationcontrollerjava.model;

import com.github.lisama569.autoscalingremediationcontrollerjava.infra.RepairMode;
import com.github.lisama569.autoscalingremediationcontrollerjava.infra.RepairPlan;

/** 修复任务（可变状态），一个实例同一时刻至多存在一个活动任务。 */
public final class RemediationTask {
    private final String taskId;
    private final String workloadId;
    private final String instanceId;
    private final long policyVersion;
    private final RepairMode mode;
    private final int attempt;
    private volatile RemediationStatus status;
    private final long createdAtMillis;
    private volatile long startedAtMillis;
    private volatile long finishedAtMillis;
    private volatile RepairPlan plan;
    private volatile String detail;

    public RemediationTask(String taskId, String workloadId, String instanceId,
                           long policyVersion, RepairMode mode, int attempt,
                           RemediationStatus status, long createdAtMillis) {
        this.taskId = taskId;
        this.workloadId = workloadId;
        this.instanceId = instanceId;
        this.policyVersion = policyVersion;
        this.mode = mode;
        this.attempt = attempt;
        this.status = status;
        this.createdAtMillis = createdAtMillis;
    }

    public String getTaskId() { return taskId; }
    public String getWorkloadId() { return workloadId; }
    public String getInstanceId() { return instanceId; }
    public long getPolicyVersion() { return policyVersion; }
    public RepairMode getMode() { return mode; }
    public int getAttempt() { return attempt; }
    public RemediationStatus getStatus() { return status; }
    public void setStatus(RemediationStatus status) { this.status = status; }
    public long getCreatedAtMillis() { return createdAtMillis; }
    public long getStartedAtMillis() { return startedAtMillis; }
    public void setStartedAtMillis(long v) { this.startedAtMillis = v; }
    public long getFinishedAtMillis() { return finishedAtMillis; }
    public void setFinishedAtMillis(long v) { this.finishedAtMillis = v; }
    public RepairPlan getPlan() { return plan; }
    public void setPlan(RepairPlan plan) { this.plan = plan; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }

    public boolean isActive() {
        return status == RemediationStatus.PENDING || status == RemediationStatus.RUNNING;
    }
}
