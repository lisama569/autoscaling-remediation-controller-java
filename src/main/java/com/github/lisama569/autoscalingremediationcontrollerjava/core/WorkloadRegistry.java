package com.github.lisama569.autoscalingremediationcontrollerjava.core;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.Clock;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Workload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 工作负载注册表。
 *
 * <p>并发模型：注册表层用一把短临界区锁保护 Map 结构；拿到 {@link Workload} 后，
 * 所有状态迁移在该工作负载自己的 monitor 上进行（{@link #withLock}），
 * 因此不同工作负载完全并行，同一工作负载严格串行，不会有交错的重复迁移。</p>
 *
 * <p>版本规则：策略带 version，只接受严格递增的版本（防止版本回退覆盖新策略）；
 * 观测带 sourceId + sequence，只接受同来源严格递增的序号。</p>
 */
@Component
public final class WorkloadRegistry {

    private final Clock clock;
    private final PolicyValidator policyValidator;
    private final DecisionLog decisionLog;
    private final Map<String, Workload> workloads = new LinkedHashMap<>();

    public WorkloadRegistry(Clock clock, PolicyValidator policyValidator, DecisionLog decisionLog) {
        this.clock = clock;
        this.policyValidator = policyValidator;
        this.decisionLog = decisionLog;
    }

    public DecisionResult register(String workloadId, ScalingPolicy policy, int initialReplicas) {
        long now = clock.nowMillis();
        if (workloadId == null || workloadId.isBlank()) {
            DecisionResult r = DecisionResult.rejected(null, "REGISTER", null,
                    RejectReason.POLICY_INVALID, "workloadId is blank", now, 0);
            decisionLog.append(r);
            return r;
        }
        List<String> errors = policyValidator.validate(policy);
        if (!errors.isEmpty()) {
            DecisionResult r = DecisionResult.rejected(workloadId, "REGISTER", null,
                    RejectReason.POLICY_INVALID, String.join("; ", errors), now, 0);
            decisionLog.append(r);
            return r;
        }
        Workload created;
        synchronized (workloads) {
            if (workloads.containsKey(workloadId)) {
                DecisionResult r = DecisionResult.rejected(workloadId, "REGISTER", null,
                        RejectReason.WORKLOAD_ALREADY_EXISTS,
                        "workload already registered; use policy update", now, 0);
                decisionLog.append(r);
                return r;
            }
            created = new Workload(workloadId, policy, initialReplicas);
            workloads.put(workloadId, created);
        }
        // 在工作负载锁内按初始副本数放入健康实例，保证初始状态确定。
        synchronized (created.monitor()) {
            for (int i = 0; i < initialReplicas; i++) {
                created.addInstance(InstanceStatus.HEALTHY);
            }
        }
        DecisionResult ok = new DecisionResult(true, workloadId, "REGISTER", 0, initialReplicas,
                null, null, RejectReason.OK, "registered", now, created.getStateVersion());
        decisionLog.append(ok);
        return ok;
    }

    public Workload get(String workloadId) {
        synchronized (workloads) {
            return workloads.get(workloadId);
        }
    }

    public List<Workload> all() {
        synchronized (workloads) {
            return new ArrayList<>(workloads.values());
        }
    }

    /**
     * 版本化策略更新。newPolicy.version 必须严格大于当前版本；
     * 更新期间不重置冷却时间与观测，且非法策略以 POLICY_INVALID 拒绝。
     */
    public DecisionResult updatePolicy(String workloadId, ScalingPolicy newPolicy) {
        long now = clock.nowMillis();
        Workload w = get(workloadId);
        if (w == null) {
            DecisionResult r = DecisionResult.rejected(workloadId, "POLICY_UPDATE", null,
                    RejectReason.WORKLOAD_NOT_FOUND, "workload is not registered", now, 0);
            decisionLog.append(r);
            return r;
        }
        List<String> errors = policyValidator.validate(newPolicy);
        if (!errors.isEmpty()) {
            DecisionResult r = DecisionResult.rejected(workloadId, "POLICY_UPDATE", null,
                    RejectReason.POLICY_INVALID, String.join("; ", errors), now,
                    w.getStateVersion());
            decisionLog.append(r);
            return r;
        }
        synchronized (w.monitor()) {
            long currentVersion = w.getPolicy().version();
            if (newPolicy.version() <= currentVersion) {
                DecisionResult r = DecisionResult.rejected(workloadId, "POLICY_UPDATE", null,
                        RejectReason.POLICY_VERSION_STALE,
                        "incoming version " + newPolicy.version()
                                + " is not greater than current version " + currentVersion,
                        now, w.getStateVersion());
                decisionLog.append(r);
                return r;
            }
            w.updatePolicy(newPolicy);
            DecisionResult ok = new DecisionResult(true, workloadId, "POLICY_UPDATE", null, null,
                    null, null, RejectReason.OK,
                    "policy version " + currentVersion + " -> " + newPolicy.version(),
                    now, w.getStateVersion());
            decisionLog.append(ok);
            return ok;
        }
    }

    /**
     * 接收一次负载观测。load 为 null 表示该来源明确上报“无负载数据”（loadPresent=false）。
     * 同来源重复序号 -> OBSERVATION_DUPLICATE；序号回退 -> OBSERVATION_STALE。
     */
    public DecisionResult receiveObservation(String workloadId, String sourceId, long sequence,
                                             Double load) {
        long now = clock.nowMillis();
        Workload w = get(workloadId);
        if (w == null) {
            DecisionResult r = DecisionResult.rejected(workloadId, "OBSERVE", null,
                    RejectReason.WORKLOAD_NOT_FOUND, "workload is not registered", now, 0);
            decisionLog.append(r);
            return r;
        }
        if (sourceId == null || sourceId.isBlank()) {
            DecisionResult r = DecisionResult.rejected(workloadId, "OBSERVE", null,
                    RejectReason.OBSERVATION_MISSING, "sourceId is blank", now,
                    w.getStateVersion());
            decisionLog.append(r);
            return r;
        }
        LoadObservation obs = stamp(sourceId, sequence, load);
        synchronized (w.monitor()) {
            int outcome = w.acceptObservation(obs);
            if (outcome == 0) {
                DecisionResult r = DecisionResult.rejected(workloadId, "OBSERVE", null,
                        RejectReason.OBSERVATION_DUPLICATE,
                        "duplicate sequence " + sequence + " from source " + sourceId,
                        now, w.getStateVersion());
                decisionLog.append(r);
                return r;
            }
            if (outcome < 0) {
                DecisionResult r = DecisionResult.rejected(workloadId, "OBSERVE", null,
                        RejectReason.OBSERVATION_STALE,
                        "stale sequence " + sequence + " from source " + sourceId
                                + " must not overwrite newer observation",
                        now, w.getStateVersion());
                decisionLog.append(r);
                return r;
            }
            DecisionResult ok = new DecisionResult(true, workloadId, "OBSERVE", null, null,
                    null, null, RejectReason.OK,
                    "source=" + sourceId + " seq=" + sequence
                            + (load == null ? " load=ABSENT" : " load=" + load),
                    now, w.getStateVersion());
            decisionLog.append(ok);
            return ok;
        }
    }

    /** 上报实例健康帧；实例不存在返回 REPAIR_INSTANCE_GONE（不做任何状态变更）。 */
    public DecisionResult reportHealth(String workloadId, String instanceId, boolean healthy) {
        long now = clock.nowMillis();
        Workload w = get(workloadId);
        if (w == null) {
            DecisionResult r = DecisionResult.rejected(workloadId, "HEALTH", instanceId,
                    RejectReason.WORKLOAD_NOT_FOUND, "workload is not registered", now, 0);
            decisionLog.append(r);
            return r;
        }
        synchronized (w.monitor()) {
            if (w.getInstance(instanceId) == null) {
                DecisionResult r = DecisionResult.rejected(workloadId, "HEALTH", instanceId,
                        RejectReason.REPAIR_INSTANCE_GONE, "instance does not exist",
                        now, w.getStateVersion());
                decisionLog.append(r);
                return r;
            }
            int streak = w.reportHealth(instanceId, healthy, now);
            DecisionResult ok = new DecisionResult(true, workloadId, "HEALTH", null, null,
                    instanceId, null, RejectReason.OK,
                    "healthy=" + healthy + " unhealthyStreak=" + streak,
                    now, w.getStateVersion());
            decisionLog.append(ok);
            return ok;
        }
    }

    /**
     * 在工作负载锁内执行一次状态读取或迁移，保证同一工作负载的判定与变更原子。
     * 工作负载不存在时返回 onMissing 的结果。
     */
    public <T> T withLock(String workloadId, Function<Workload, T> action,
                          Supplier<T> onMissing) {
        Workload w = get(workloadId);
        if (w == null) {
            return onMissing.get();
        }
        synchronized (w.monitor()) {
            return action.apply(w);
        }
    }

    public LoadObservation stamp(String sourceId, long sequence, Double load) {
        return new LoadObservation(sourceId, sequence,
                load == null ? 0.0d : load, load != null, clock.nowMillis());
    }
}
