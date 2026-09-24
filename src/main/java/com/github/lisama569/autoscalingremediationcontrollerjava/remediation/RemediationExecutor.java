package com.github.lisama569.autoscalingremediationcontrollerjava.remediation;

import com.github.lisama569.autoscalingremediationcontrollerjava.infra.RepairMode;
import com.github.lisama569.autoscalingremediationcontrollerjava.infra.RepairPlan;
import com.github.lisama569.autoscalingremediationcontrollerjava.infra.SimulatedInfrastructure;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationTask;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.orchestration.EventLog;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadAggregate;
import com.github.lisama569.autoscalingremediationcontrollerjava.time.Clock;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 修复任务执行器（在调用方持有工作负载串行锁时执行）。
 *
 * <p>生命周期：PENDING（已建任务，等待并发槽）→ RUNNING（计划已确定）→
 * SUCCEEDED / FAILED / TIMED_OUT。失败与超时都计入失败预算；预算窗口滚动过期后恢复。
 * 不做自动重试：终止即终态，再次修复必须由显式请求重新建任务，因此不存在无限重试或重复任务。
 */
@Component
public class RemediationExecutor {

    private final SimulatedInfrastructure infra;
    private final Clock clock;
    private final EventLog eventLog;

    public RemediationExecutor(SimulatedInfrastructure infra, Clock clock, EventLog eventLog) {
        this.infra = infra;
        this.clock = clock;
        this.eventLog = eventLog;
    }

    /** 当前 RUNNING 任务数（PENDING 不占槽）。 */
    public int activeRunningCount(WorkloadAggregate agg) {
        int n = 0;
        for (RemediationTask t : agg.tasksByInstance().values()) {
            if (t.getStatus() == RemediationStatus.RUNNING) {
                n++;
            }
        }
        return n;
    }

    /** 滚动清理预算窗口外的失败记录，返回当前窗口内失败次数。 */
    public int budgetFailures(WorkloadAggregate agg, long nowMillis) {
        long window = agg.getPolicy().failureBudgetWindowMillis();
        agg.failureTimes().removeIf(t -> nowMillis - t > window);
        return agg.failureTimes().size();
    }

    /**
     * 推进一个工作负载的全部修复任务。
     * 先启动符合条件的 PENDING 任务（受并发槽限制），再检查 RUNNING 任务的完成/超时。
     * 返回本轮产生的终态迁移（用于日志与测试断言）。
     */
    public List<RemediationTask> tick(WorkloadAggregate agg, long nowMillis) {
        List<RemediationTask> finished = new ArrayList<>();
        ScalePolicy policy = agg.getPolicy();

        startPending(agg, policy, nowMillis);

        for (RemediationTask task : new ArrayList<>(agg.tasksByInstance().values())) {
            if (task.getStatus() != RemediationStatus.RUNNING) {
                continue;
            }
            RepairPlan plan = task.getPlan();
            boolean timedOut = nowMillis - task.getStartedAtMillis() >= policy.remediationTimeoutMillis();
            boolean plannedDue = nowMillis - task.getStartedAtMillis() >= plan.durationMillis();

            if (timedOut && (!plannedDue || plan.timedOut())) {
                // 超时优先：即使计划声称成功，只要超过超时时间也按超时终止（不应用成功副作用）
                Instance timingOutInstance = agg.instances().get(task.getInstanceId());
                finishFailure(agg, task, RemediationStatus.TIMED_OUT, timingOutInstance, nowMillis,
                        "repair timed out after " + policy.remediationTimeoutMillis() + "ms");
                finished.add(task);
                continue;
            }
            if (plannedDue) {
                completePlanned(agg, task, plan, nowMillis, finished);
            }
        }
        return finished;
    }

    private void startPending(WorkloadAggregate agg, ScalePolicy policy, long nowMillis) {
        int slots = policy.remediationConcurrency() - activeRunningCount(agg);
        if (slots <= 0) {
            return;
        }
        for (RemediationTask task : agg.tasksByInstance().values()) {
            if (slots <= 0) {
                break;
            }
            if (task.getStatus() != RemediationStatus.PENDING) {
                continue;
            }
            Instance instance = agg.instances().get(task.getInstanceId());
            if (instance == null) {
                finishFailure(agg, task, RemediationStatus.FAILED, null, nowMillis,
                        "instance disappeared before start");
                continue;
            }
            instance.setStatus(InstanceStatus.REPAIRING);
            RepairPlan plan = infra.plan(instance);
            task.setPlan(plan);
            task.setStatus(RemediationStatus.RUNNING);
            task.setStartedAtMillis(nowMillis);
            slots--;
            eventLog.log(nowMillis, "remediation START task=" + task.getTaskId()
                    + " workload=" + agg.getWorkloadId()
                    + " instance=" + instance.getId() + " mode=" + task.getMode()
                    + " plannedDuration=" + plan.durationMillis()
                    + " success=" + plan.success() + " timedOut=" + plan.timedOut());
        }
    }

    private void completePlanned(WorkloadAggregate agg, RemediationTask task, RepairPlan plan,
                                 long nowMillis, List<RemediationTask> finished) {
        Instance instance = agg.instances().get(task.getInstanceId());
        if (plan.timedOut()) {
            finishFailure(agg, task, RemediationStatus.TIMED_OUT, instance, nowMillis,
                    "scripted timeout duration=" + plan.durationMillis());
            finished.add(task);
            return;
        }
        RepairMode mode = task.getMode();
        var execution = infra.completeRepair(instance, mode, plan.success(), nowMillis);
        if (!execution.success()) {
            finishFailure(agg, task, RemediationStatus.FAILED, instance, nowMillis, execution.detail());
            finished.add(task);
            return;
        }
        task.setStatus(RemediationStatus.SUCCEEDED);
        task.setFinishedAtMillis(nowMillis);
        task.setDetail(execution.detail());
        if (mode == RepairMode.REPLACE && execution.replacedInstanceId() != null) {
            swapReplacement(agg, instance, execution.replacedInstanceId(), nowMillis);
        }
        eventLog.log(nowMillis, "remediation SUCCESS task=" + task.getTaskId()
                + " instance=" + task.getInstanceId() + " mode=" + mode
                + (execution.replacedInstanceId() != null ? " replacement=" + execution.replacedInstanceId() : "")
                + " detail=" + execution.detail());
        finished.add(task);
    }

    private void swapReplacement(WorkloadAggregate agg, Instance oldInstance, String newId, long nowMillis) {
        Instance replacement = new Instance(newId, agg.getWorkloadId(), InstanceStatus.READY, nowMillis);
        agg.instances().remove(oldInstance.getId());
        agg.instances().put(newId, replacement);
        // 旧实例任务已终止，新实例没有任何任务与健康历史
    }

    private void finishFailure(WorkloadAggregate agg, RemediationTask task, RemediationStatus status,
                               Instance instance, long nowMillis, String detail) {
        task.setStatus(status);
        task.setFinishedAtMillis(nowMillis);
        task.setDetail(detail);
        agg.failureTimes().addLast(nowMillis);
        if (instance != null) {
            instance.setStatus(InstanceStatus.UNHEALTHY);
        }
        eventLog.log(nowMillis, "remediation " + status + " task=" + task.getTaskId()
                + " instance=" + task.getInstanceId() + " detail=" + detail
                + " failuresInWindow=" + agg.failureTimes().size());
    }
}
