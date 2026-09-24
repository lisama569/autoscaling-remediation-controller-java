package com.github.lisama569.autoscalingremediationcontrollerjava.remediation;

import com.github.lisama569.autoscalingremediationcontrollerjava.infra.RepairMode;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationRequestOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationRequestResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationTask;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.orchestration.EventLog;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadAggregate;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadRegistry;
import com.github.lisama569.autoscalingremediationcontrollerjava.time.Clock;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * 修复请求入口（在调用方持有工作负载串行锁时执行）。
 *
 * <p>请求只负责“是否允许建任务”，所有拒绝原因均可区分：
 * 未注册 / 无策略 / 策略禁止 / 实例不存在或已终止 / 实例健康 / 已有活动任务（重复）/
 * 并发槽已满 / 失败预算耗尽 / 实例级冷却中。
 * 任务实际执行与终态由 {@link RemediationExecutor#tick} 推进。
 */
@Component
public class RemediationService {

    private final WorkloadRegistry registry;
    private final RemediationExecutor executor;
    private final Clock clock;
    private final EventLog eventLog;

    public RemediationService(WorkloadRegistry registry, RemediationExecutor executor,
                              Clock clock, EventLog eventLog) {
        this.registry = registry;
        this.executor = executor;
        this.clock = clock;
        this.eventLog = eventLog;
    }

    public RemediationRequestOutcome requestRemediation(String workloadId, String instanceId) {
        return requestRemediation(workloadId, instanceId, clock.nowMillis());
    }

    public RemediationRequestOutcome requestRemediation(String workloadId, String instanceId, long nowMillis) {
        WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
        if (agg == null) {
            return reject(workloadId, instanceId, null,
                    RejectionReason.WORKLOAD_NOT_REGISTERED, "workload not registered: " + workloadId);
        }
        ScalePolicy policy = agg.getPolicy();
        if (policy == null) {
            return reject(workloadId, instanceId, null,
                    RejectionReason.MISSING_POLICY, "no policy attached to workload " + workloadId);
        }
        if (!policy.remediationEnabled()) {
            return reject(workloadId, instanceId, null,
                    RejectionReason.REMEDIATION_DISABLED,
                    "remediation disabled by policy v" + policy.version());
        }
        Instance instance = agg.instances().get(instanceId);
        if (instance == null || instance.getStatus() == InstanceStatus.TERMINATED) {
            return reject(workloadId, instanceId, null,
                    RejectionReason.INSTANCE_NOT_FOUND,
                    "instance not found or terminated: " + instanceId);
        }

        RemediationTask existing = agg.tasksByInstance().get(instanceId);
        if (existing != null && existing.isActive()) {
            eventLog.log(nowMillis, "remediation DUPLICATE workload=" + workloadId
                    + " instance=" + instanceId + " existingTask=" + existing.getTaskId()
                    + " status=" + existing.getStatus());
            return new RemediationRequestOutcome(workloadId, instanceId,
                    RemediationRequestResult.DUPLICATE, existing.getTaskId(),
                    RejectionReason.DUPLICATE_REMEDIATION,
                    "active remediation already exists: " + existing.getTaskId());
        }

        if (instance.getStatus() == InstanceStatus.READY) {
            return reject(workloadId, instanceId, null,
                    RejectionReason.INSTANCE_HEALTHY,
                    "instance " + instanceId + " is READY, remediation not needed");
        }

        int failures = executor.budgetFailures(agg, nowMillis);
        if (failures >= policy.failureBudget()) {
            return reject(workloadId, instanceId, null,
                    RejectionReason.FAILURE_BUDGET_EXHAUSTED,
                    "failure budget exhausted: " + failures + " failures within "
                            + policy.failureBudgetWindowMillis() + "ms window (budget="
                            + policy.failureBudget() + ")");
        }

        if (existing != null) {
            long wait = nowMillis - existing.getFinishedAtMillis();
            if (wait < policy.remediationCooldownMillis()) {
                return reject(workloadId, instanceId, existing.getTaskId(),
                        RejectionReason.REMEDIATION_COOLDOWN_ACTIVE,
                        "instance cooldown active: "
                                + (policy.remediationCooldownMillis() - wait) + "ms remaining");
            }
        }

        if (executor.activeRunningCount(agg) >= policy.remediationConcurrency()) {
            return reject(workloadId, instanceId, null,
                    RejectionReason.REMEDIATION_CONCURRENCY_LIMIT,
                    "remediation concurrency limit reached: " + policy.remediationConcurrency());
        }

        int attempt = (existing == null) ? 1 : existing.getAttempt() + 1;
        RepairMode mode = (attempt == 1) ? RepairMode.REPAIR : RepairMode.REPLACE;
        String taskId = workloadId + "-rm-" + UUID.randomUUID();
        RemediationTask task = new RemediationTask(taskId, workloadId, instanceId,
                policy.version(), mode, attempt, RemediationStatus.PENDING, nowMillis);
        agg.tasksByInstance().put(instanceId, task);
        agg.taskHistory().add(task);

        eventLog.log(nowMillis, "remediation ACCEPT workload=" + workloadId
                + " instance=" + instanceId + " task=" + taskId + " attempt=" + attempt
                + " mode=" + mode + " budgetFailures=" + failures + "/" + policy.failureBudget());

        // 立即尝试启动（调用方持锁，tick 与请求互斥）
        executor.tick(agg, nowMillis);
        return new RemediationRequestOutcome(workloadId, instanceId,
                RemediationRequestResult.ACCEPTED, taskId, null,
                "remediation accepted attempt=" + attempt + " mode=" + mode);
    }

    private static RemediationRequestOutcome reject(String workloadId, String instanceId, String taskId,
                                                    RejectionReason reason, String rationale) {
        return new RemediationRequestOutcome(workloadId, instanceId,
                RemediationRequestResult.REJECTED, taskId, reason, rationale);
    }
}
