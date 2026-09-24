package com.github.lisama569.autoscalingremediationcontrollerjava.domain;

/**
 * 一次修复任务。同一实例任意时刻至多有一个排队/执行中的任务（幂等键）。
 * 终态原因用于区分超时、脚本失败、尝试次数耗尽等可区分结果。
 */
public final class RepairTask {

    private final long taskId;
    private final String workloadId;
    private final String instanceId;
    private final boolean replace;
    private final long enqueuedAt;
    private long startedAt = -1L;
    private long deadlineAt = -1L;
    private int attempts;
    private String terminalReason;

    public RepairTask(long taskId, String workloadId, String instanceId,
                      boolean replace, long enqueuedAt) {
        this.taskId = taskId;
        this.workloadId = workloadId;
        this.instanceId = instanceId;
        this.replace = replace;
        this.enqueuedAt = enqueuedAt;
    }

    public long getTaskId() { return taskId; }
    public String getWorkloadId() { return workloadId; }
    public String getInstanceId() { return instanceId; }
    public boolean isReplace() { return replace; }
    public long getEnqueuedAt() { return enqueuedAt; }
    public long getStartedAt() { return startedAt; }
    public long getDeadlineAt() { return deadlineAt; }
    public int getAttempts() { return attempts; }
    public String getTerminalReason() { return terminalReason; }
    public boolean isTerminal() { return terminalReason != null; }

    public void markStarted(long now, long timeoutMillis) {
        this.startedAt = now;
        this.deadlineAt = now + timeoutMillis;
        this.attempts++;
    }

    public void incrementAttempts() {
        this.attempts++;
    }

    public void terminate(String reason) {
        this.terminalReason = reason;
    }

    @Override
    public String toString() {
        return "RepairTask{taskId=" + taskId
                + ", workload=" + workloadId
                + ", instance=" + instanceId
                + ", replace=" + replace
                + ", attempts=" + attempts
                + ", startedAt=" + startedAt
                + ", deadlineAt=" + deadlineAt
                + (terminalReason == null ? ", state=ACTIVE}" : ", terminal=" + terminalReason + '}');
    }
}
