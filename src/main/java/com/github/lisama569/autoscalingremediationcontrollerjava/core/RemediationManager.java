package com.github.lisama569.autoscalingremediationcontrollerjava.core;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.Clock;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.RepairOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.RepairTask;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Workload;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自愈编排。
 *
 * <p>每个工作负载维护一条 FIFO 修复队列与一组执行中任务；全部状态迁移都在
 * 对应工作负载的 monitor 内完成，配合“同一实例任意时刻至多一个排队/执行任务”
 * 的幂等规则，保证重复触发、并发交错下不会出现重复修复或无限重试。</p>
 *
 * <p>约束（均产生可区分结果）：策略禁止 -> REMEDIATION_DISABLED；
 * 已排队/执行中 -> REM_ALREADY_QUEUED / REM_ALREADY_IN_PROGRESS；
 * 实例修复冷却 -> REM_IN_REMEDIATION_COOLDOWN；并发上限 -> REM_QUEUED（入队等待）；
 * 预算耗尽 -> REM_FAILURE_BUDGET_EXHAUSTED（拒绝且不入队）；
 * 执行终态 -> REPAIR_TIMEOUT / REPAIR_FAILED / REPAIR_ATTEMPTS_EXHAUSTED / REPAIR_INSTANCE_GONE。
 * 预算按 failureBudgetWindowMillis 滑动窗口计失败次数，窗口滑过后自动恢复。</p>
 */
@Component
public final class RemediationManager {

    private final WorkloadRegistry registry;
    private final Clock clock;
    private final DecisionLog decisionLog;
    private final AtomicLong taskIdSeq = new AtomicLong(1);
    private volatile RepairBehavior behavior = RepairBehavior.ALWAYS_REPAIRED;
    private volatile int maxAttempts = 3;

    /** state 自身只在持有对应 Workload monitor 时访问。 */
    private final Map<String, State> states = new LinkedHashMap<>();

    public RemediationManager(WorkloadRegistry registry, Clock clock, DecisionLog decisionLog) {
        this.registry = registry;
        this.clock = clock;
        this.decisionLog = decisionLog;
    }

    public void setBehavior(RepairBehavior behavior) {
        this.behavior = behavior == null ? RepairBehavior.ALWAYS_REPAIRED : behavior;
    }

    public void setMaxAttempts(int maxAttempts) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive: " + maxAttempts);
        }
        this.maxAttempts = maxAttempts;
    }

    /** 扫描所有达到不健康门槛的实例并逐一生成修复决策（顺序确定）。 */
    public List<DecisionResult> scan(String workloadId) {
        List<DecisionResult> out = new ArrayList<>();
        Workload w = registry.get(workloadId);
        if (w == null) {
            long now = clock.nowMillis();
            DecisionResult r = DecisionResult.rejected(workloadId, "REMEDIATE", null,
                    RejectReason.WORKLOAD_NOT_FOUND, "workload is not registered", now, 0);
            decisionLog.append(r);
            out.add(r);
            return out;
        }
        List<String> targets;
        synchronized (w.monitor()) {
            targets = w.eligibleUnhealthyInstances();
        }
        for (String instanceId : targets) {
            out.add(trigger(workloadId, instanceId, "SCAN"));
        }
        return out;
    }

    public DecisionResult trigger(String workloadId, String instanceId, String source) {
        long now = clock.nowMillis();
        Workload w = registry.get(workloadId);
        if (w == null) {
            DecisionResult r = DecisionResult.rejected(workloadId, "REMEDIATE", instanceId,
                    RejectReason.WORKLOAD_NOT_FOUND, "workload is not registered", now, 0);
            decisionLog.append(r);
            return r;
        }
        synchronized (w.monitor()) {
            ScalingPolicy p = w.getPolicy();
            State st = states.computeIfAbsent(workloadId, k -> new State());
            pruneFailures(st, p, now);

            if (!p.remediationEnabled()) {
                return reject(w, instanceId, RejectReason.REMEDIATION_DISABLED,
                        "remediation disabled by policy version " + p.version(), now);
            }
            Instance instance = w.getInstance(instanceId);
            if (instance == null) {
                return reject(w, instanceId, RejectReason.REPAIR_INSTANCE_GONE,
                        "instance does not exist", now);
            }
            if (st.active.containsKey(instanceId)) {
                return reject(w, instanceId, RejectReason.REM_ALREADY_IN_PROGRESS,
                        "taskId=" + st.active.get(instanceId).getTaskId()
                                + " already repairing this instance", now);
            }
            for (RepairTask t : st.queue) {
                if (t.getInstanceId().equals(instanceId)) {
                    return reject(w, instanceId, RejectReason.REM_ALREADY_QUEUED,
                            "taskId=" + t.getTaskId() + " already queued for this instance", now);
                }
            }
            InstanceStatus s = instance.getStatus();
            if (s == InstanceStatus.HEALTHY) {
                return reject(w, instanceId, RejectReason.REM_NO_UNHEALTHY_INSTANCE,
                        "instance is healthy; nothing to remediate", now);
            }
            if (s == InstanceStatus.REPAIRING) {
                return reject(w, instanceId, RejectReason.REM_ALREADY_IN_PROGRESS,
                        "instance is already being repaired", now);
            }
            if (s == InstanceStatus.REPLACED || s == InstanceStatus.TERMINATED) {
                return reject(w, instanceId, RejectReason.REPAIR_INSTANCE_GONE,
                        "instance has already been " + s, now);
            }
            // REMEDIATION_FAILED / UNHEALTHY 均允许在冷却期过后重新发起修复。
            long lastAt = w.lastRemediationAtOf(instanceId);
            if (now - lastAt < p.repairCooldownMillis()) {
                return reject(w, instanceId, RejectReason.REM_IN_REMEDIATION_COOLDOWN,
                        "repair cooldown remaining "
                                + (p.repairCooldownMillis() - (now - lastAt)) + "ms", now);
            }
            if (remainingFailureBudgetLocked(st, p, now) <= 0) {
                return reject(w, instanceId, RejectReason.REM_FAILURE_BUDGET_EXHAUSTED,
                        "failure budget " + p.failureBudget() + " exhausted within window "
                                + p.failureBudgetWindowMillis() + "ms; "
                                + st.failures.size() + " recent failures", now);
            }

            RepairTask task = new RepairTask(taskIdSeq.getAndIncrement(),
                    workloadId, instanceId, false, now);
            if (st.active.size() >= p.maxConcurrentRepairs()) {
                st.queue.addLast(task);
                DecisionResult r = DecisionResult.queued(workloadId, instanceId, task.getTaskId(),
                        "queued due to concurrency limit " + p.maxConcurrentRepairs()
                                + "; source=" + source, now, w.getStateVersion());
                decisionLog.append(r);
                return r;
            }
            startTask(w, st, task, now);
            DecisionResult r = DecisionResult.queued(workloadId, instanceId, task.getTaskId(),
                    "repair started; source=" + source, now, w.getStateVersion());
            decisionLog.append(r);
            return r;
        }
    }

    private void startTask(Workload w, State st, RepairTask task, long now) {
        ScalingPolicy p = w.getPolicy();
        task.markStarted(now, p.repairTimeoutMillis());
        st.active.put(task.getInstanceId(), task);
        w.changeStatus(task.getInstanceId(), InstanceStatus.REPAIRING);
    }

    /**
     * 推进所有工作负载：先判定执行中任务的超时/脚本结果，再按 FIFO 晋升排队任务。
     * 返回本轮产生的全部终态决策（顺序按工作负载 id 与队列顺序确定）。
     */
    public List<DecisionResult> pump() {
        long now = clock.nowMillis();
        List<DecisionResult> out = new ArrayList<>();
        List<Workload> ordered = registry.all();
        ordered.sort((a, b) -> a.getId().compareTo(b.getId()));
        for (Workload w : ordered) {
            synchronized (w.monitor()) {
                State st = states.get(w.getId());
                if (st == null) {
                    continue;
                }
                pruneFailures(st, w.getPolicy(), now);
                // 阶段一：本拍开始前已在执行的任务，按 taskId 顺序处理。
                List<RepairTask> active = new ArrayList<>(st.active.values());
                active.sort((a, b) -> Long.compare(a.getTaskId(), b.getTaskId()));
                for (RepairTask t : active) {
                    if (!t.isTerminal() && st.active.containsKey(t.getInstanceId())) {
                        out.add(executeLocked(w, st, t, now));
                    }
                }
                // 阶段二：反复“晋升队首 -> 立即执行”，直到没有可晋升任务。
                // 终态任务里的级联晋升调用不会再执行任务（保持由本循环统一驱动）。
                ScalingPolicy p = w.getPolicy();
                while (st.active.size() < p.maxConcurrentRepairs() && !st.queue.isEmpty()) {
                    RepairTask next = st.queue.peekFirst();
                    Instance in = w.getInstance(next.getInstanceId());
                    if (in == null || in.getStatus() == InstanceStatus.HEALTHY
                            || in.getStatus() == InstanceStatus.REPLACED
                            || in.getStatus() == InstanceStatus.TERMINATED) {
                        st.queue.pollFirst();
                        next.terminate("CANCELED: instance no longer needs remediation");
                        decisionLog.append(DecisionResult.noop(w.getId(), "REPAIR",
                                RejectReason.REPAIR_INSTANCE_GONE,
                                "queued taskId=" + next.getTaskId() + " for instance "
                                        + next.getInstanceId()
                                        + " canceled: instance recovered/gone",
                                now, w.getStateVersion()));
                        continue;
                    }
                    if (remainingFailureBudgetLocked(st, p, now) <= 0) {
                        break;
                    }
                    st.queue.pollFirst();
                    startTask(w, st, next, now);
                    decisionLog.append(DecisionResult.queued(w.getId(), next.getInstanceId(),
                            next.getTaskId(), "queued repair promoted to active", now,
                            w.getStateVersion()));
                    out.add(executeLocked(w, st, next, now));
                }
            }
        }
        return out;
    }

    private void promoteQueue(Workload w, State st, long now, List<DecisionResult> out) {
        // 终态路径下只需尝试释放阻塞；真正的“晋升 + 执行”由 pump 的阶段二统一驱动，
        // 避免同一拍内重复执行、保证顺序确定。
    }

    /** 对指定任务执行一轮（主要供测试直接驱动），必须在工作负载 monitor 外调用。 */
    public DecisionResult executeOne(String workloadId, long taskId) {
        long now = clock.nowMillis();
        Workload w = registry.get(workloadId);
        if (w == null) {
            DecisionResult r = DecisionResult.rejected(workloadId, "REPAIR", null,
                    RejectReason.WORKLOAD_NOT_FOUND, "workload is not registered", now, 0);
            decisionLog.append(r);
            return r;
        }
        synchronized (w.monitor()) {
            State st = states.get(workloadId);
            RepairTask task = findTask(st, taskId);
            if (task == null) {
                return reject(w, null, RejectReason.NO_QUEUED_TASK,
                        "taskId=" + taskId + " not found", now);
            }
            return executeLocked(w, st, task, now);
        }
    }

    private RepairTask findTask(State st, long taskId) {
        if (st == null) {
            return null;
        }
        for (RepairTask t : st.active.values()) {
            if (t.getTaskId() == taskId) {
                return t;
            }
        }
        for (RepairTask t : st.queue) {
            if (t.getTaskId() == taskId) {
                return t;
            }
        }
        return null;
    }

    private DecisionResult executeLocked(Workload w, State st, RepairTask task, long now) {
        ScalingPolicy p = w.getPolicy();
        String iid = task.getInstanceId();

        if (now >= task.getDeadlineAt()) {
            return finishFailure(w, st, task, RejectReason.REPAIR_TIMEOUT,
                    "repair deadline " + task.getDeadlineAt() + " reached", now, true);
        }
        pruneFailures(st, p, now);
        if (remainingFailureBudgetLocked(st, p, now) <= 0) {
            return finishFailure(w, st, task, RejectReason.REM_FAILURE_BUDGET_EXHAUSTED,
                    "failure budget exhausted during repair", now, true);
        }

        RepairOutcome outcome;
        try {
            outcome = behavior.outcomeFor(w.getId(), iid, task.getAttempts(), now);
        } catch (RuntimeException ex) {
            outcome = RepairOutcome.FAILED;
        }

        switch (outcome) {
            case REPAIRED -> {
                st.active.remove(iid);
                task.terminate(RejectReason.OK.name());
                w.recordRemediation(iid, now);
                w.reportHealth(iid, true, now);
                Instance in = w.getInstance(iid);
                if (in != null) {
                    in.setStatus(InstanceStatus.HEALTHY);
                }
                DecisionResult r = DecisionResult.repairTerminal(w.getId(), iid, task.getTaskId(),
                        true, RejectReason.OK, "repaired after " + task.getAttempts()
                                + " attempt(s)", now, w.getStateVersion());
                decisionLog.append(r);
                promoteQueue(w, st, now, new ArrayList<>());
                return r;
            }
            case REPLACED -> {
                st.active.remove(iid);
                task.terminate(RejectReason.OK.name());
                w.recordRemediation(iid, now);
                w.changeStatus(iid, InstanceStatus.REPLACED);
                String nid = w.addInstance(InstanceStatus.HEALTHY);
                DecisionResult r = DecisionResult.repairTerminal(w.getId(), iid, task.getTaskId(),
                        true, RejectReason.OK, "replaced by new instance " + nid
                                + " after " + task.getAttempts() + " attempt(s)", now,
                        w.getStateVersion());
                decisionLog.append(r);
                promoteQueue(w, st, now, new ArrayList<>());
                return r;
            }
            case PENDING -> {
                return DecisionResult.noop(w.getId(), "REPAIR", RejectReason.OK,
                        "taskId=" + task.getTaskId() + " still in progress (deadline "
                                + task.getDeadlineAt() + ")", now, w.getStateVersion());
            }
            case TIMEOUT -> {
                return finishFailure(w, st, task, RejectReason.REPAIR_TIMEOUT,
                        "repair script reported timeout", now, true);
            }
            case FAILED -> {
                if (task.getAttempts() >= maxAttempts) {
                    return finishFailure(w, st, task, RejectReason.REPAIR_ATTEMPTS_EXHAUSTED,
                            "repair failed " + task.getAttempts() + " time(s); giving up",
                            now, true);
                }
                task.incrementAttempts();
                st.failures.add(now);
                w.changeStatus(iid, InstanceStatus.UNHEALTHY);
                // 未到最大尝试次数：任务保持 active，下一轮重试，实例不产生重复任务。
                DecisionResult r = DecisionResult.repairTerminal(w.getId(), iid, task.getTaskId(),
                        true, RejectReason.OK, "repair attempt failed; retry "
                                + task.getAttempts() + "/" + maxAttempts, now,
                        w.getStateVersion());
                decisionLog.append(r);
                return r;
            }
            default -> throw new IllegalStateException("unknown outcome: " + outcome);
        }
    }

    private DecisionResult finishFailure(Workload w, State st, RepairTask task,
                                         RejectReason reason, String detail, long now,
                                         boolean countBudget) {
        String iid = task.getInstanceId();
        if (countBudget) {
            st.failures.add(now);
        }
        st.active.remove(iid);
        task.terminate(reason.name());
        w.recordRemediation(iid, now);
        w.changeStatus(iid, InstanceStatus.REMEDIATION_FAILED);
        DecisionResult r = DecisionResult.repairTerminal(w.getId(), iid, task.getTaskId(),
                false, reason, detail, now, w.getStateVersion());
        decisionLog.append(r);
        promoteQueue(w, st, now, new ArrayList<>());
        return r;
    }

    private void pruneFailures(State st, ScalingPolicy p, long now) {
        long window = p.failureBudgetWindowMillis();
        st.failures.removeIf(t -> window > 0 && now - t > window);
    }

    private int remainingFailureBudgetLocked(State st, ScalingPolicy p, long now) {
        pruneFailures(st, p, now);
        return p.failureBudget() - st.failures.size();
    }

    public int activeRepairCount(String workloadId) {
        Workload w = registry.get(workloadId);
        if (w == null) {
            return 0;
        }
        synchronized (w.monitor()) {
            State st = states.get(workloadId);
            return st == null ? 0 : st.active.size();
        }
    }

    public int queuedRepairCount(String workloadId) {
        Workload w = registry.get(workloadId);
        if (w == null) {
            return 0;
        }
        synchronized (w.monitor()) {
            State st = states.get(workloadId);
            return st == null ? 0 : st.queue.size();
        }
    }

    public int remainingFailureBudget(String workloadId) {
        Workload w = registry.get(workloadId);
        if (w == null) {
            return 0;
        }
        synchronized (w.monitor()) {
            State st = states.computeIfAbsent(workloadId, k -> new State());
            return remainingFailureBudgetLocked(st, w.getPolicy(), clock.nowMillis());
        }
    }

    private DecisionResult reject(Workload w, String instanceId, RejectReason reason,
                                  String detail, long now) {
        DecisionResult r = DecisionResult.rejected(w.getId(), "REMEDIATE", instanceId, reason,
                detail, now, w.getStateVersion());
        decisionLog.append(r);
        return r;
    }

    /** 每工作负载的修复状态：仅在持有 Workload monitor 时访问。 */
    private static final class State {
        private final Deque<RepairTask> queue = new ArrayDeque<>();
        private final Map<String, RepairTask> active = new LinkedHashMap<>();
        private final Deque<Long> failures = new ArrayDeque<>();
    }
}
