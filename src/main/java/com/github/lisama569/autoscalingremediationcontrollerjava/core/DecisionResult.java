package com.github.lisama569.autoscalingremediationcontrollerjava.core;

/**
 * 一次编排判定的统一结果。accepted=false 时 reason 给出可区分的拒绝/终止原因。
 */
public record DecisionResult(
        boolean accepted,
        String workloadId,
        String kind,
        Integer fromReplicas,
        Integer toReplicas,
        String instanceId,
        Long taskId,
        RejectReason reason,
        String detail,
        long atMillis,
        long stateVersion
) {
    public static DecisionResult rejected(String workloadId, String kind, RejectReason reason,
                                          String detail, long now, long stateVersion) {
        return new DecisionResult(false, workloadId, kind, null, null, null, null,
                reason, detail, now, stateVersion);
    }

    public static DecisionResult scale(String workloadId, int from, int to,
                                       long now, long stateVersion) {
        return new DecisionResult(true, workloadId, "SCALE", from, to, null, null,
                RejectReason.OK, "scale", now, stateVersion);
    }

    public static DecisionResult noop(String workloadId, String kind, RejectReason reason,
                                      String detail, long now, long stateVersion) {
        return new DecisionResult(true, workloadId, kind, null, null, null, null,
                reason, detail, now, stateVersion);
    }

    /** 修复入队被接受（尚未执行）。 */
    public static DecisionResult queued(String workloadId, String instanceId, long taskId,
                                        String detail, long now, long stateVersion) {
        return new DecisionResult(true, workloadId, "REMEDIATE", null, null, instanceId, taskId,
                RejectReason.OK, detail, now, stateVersion);
    }

    /** 修复执行终态：成功替换/恢复，或可区分的失败终态。 */
    public static DecisionResult repairTerminal(String workloadId, String instanceId, long taskId,
                                                boolean success, RejectReason reason,
                                                String detail, long now, long stateVersion) {
        return new DecisionResult(success, workloadId, "REPAIR", null, null, instanceId, taskId,
                reason, detail, now, stateVersion);
    }

    public static DecisionResult rejected(String workloadId, String kind, String instanceId,
                                          RejectReason reason, String detail,
                                          long now, long stateVersion) {
        return new DecisionResult(false, workloadId, kind, null, null, instanceId, null,
                reason, detail, now, stateVersion);
    }
}
