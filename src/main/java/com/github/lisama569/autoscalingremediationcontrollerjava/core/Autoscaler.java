package com.github.lisama569.autoscalingremediationcontrollerjava.core;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.Clock;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Workload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 自动伸缩编排。
 *
 * <p>每次 evaluate 都在工作负载 monitor 内完成“读最新观测 -> 计算目标 -> 校验边界/冷却
 * -> 提交迁移”全过程，因此并发触发与重复事件只会被串行化成一次有效决策：
 * 后续触发看到的是已更新的冷却时间与副本数，不会超额扩容、漏扩容或重复扩容。</p>
 *
 * <p>判定输入不足时绝不给模糊结果：无观测 -> OBSERVATION_MISSING；
 * 观测明确无负载数据 -> 同因；观测超龄 -> OBSERVATION_STALE；
 * 冷却中 -> SCALE_IN_COOLDOWN；触及上下界 -> SCALE_AT_MIN/MAX_REPLICAS。</p>
 */
@Component
public final class Autoscaler {

    private final WorkloadRegistry registry;
    private final Clock clock;
    private final DecisionLog decisionLog;

    public Autoscaler(WorkloadRegistry registry, Clock clock, DecisionLog decisionLog) {
        this.registry = registry;
        this.clock = clock;
        this.decisionLog = decisionLog;
    }

    public DecisionResult evaluate(String workloadId) {
        long now = clock.nowMillis();
        return registry.withLock(workloadId,
                w -> evaluateLocked(w, now),
                () -> {
                    DecisionResult r = DecisionResult.rejected(workloadId, "SCALE", null,
                            RejectReason.WORKLOAD_NOT_FOUND,
                            "workload is not registered", now, 0);
                    decisionLog.append(r);
                    return r;
                });
    }

    private DecisionResult evaluateLocked(Workload w, long now) {
        ScalingPolicy p = w.getPolicy();
        LoadObservation obs = w.getLatestObservation();
        if (obs == null || !obs.loadPresent()) {
            RejectReason reason = (obs == null)
                    ? RejectReason.OBSERVATION_MISSING
                    : RejectReason.OBSERVATION_MISSING;
            DecisionResult r = DecisionResult.rejected(w.getId(), "SCALE", null, reason,
                    obs == null ? "no load observation has ever been received"
                            : "latest observation carries no load value",
                    now, w.getStateVersion());
            decisionLog.append(r);
            return r;
        }
        if (p.observationMaxAgeMillis() > 0
                && now - obs.observedAt() > p.observationMaxAgeMillis()) {
            DecisionResult r = DecisionResult.rejected(w.getId(), "SCALE", null,
                    RejectReason.OBSERVATION_STALE,
                    "latest observation age " + (now - obs.observedAt())
                            + "ms exceeds maxAge " + p.observationMaxAgeMillis() + "ms",
                    now, w.getStateVersion());
            decisionLog.append(r);
            return r;
        }

        int current = w.getCurrentReplicas();
        int desiredRaw = computeDesiredReplicasRaw(current, obs.observedLoad(), p);
        int desired = clamp(desiredRaw, p.minReplicas(), p.maxReplicas());
        String basis = "load=" + obs.observedLoad() + " replicas=" + current
                + " avgPerReplica=" + avg(obs.observedLoad(), current)
                + " upThreshold=" + p.scaleUpThreshold()
                + " downThreshold=" + p.scaleDownThreshold()
                + " desiredRaw=" + desiredRaw + " desiredClamped=" + desired;

        // 方向与边界判断都基于未 clamp 的原始目标：已在上限但仍需更多容量时，
        // 必须给出可区分的 SCALE_AT_MAX_REPLICAS，而不是被 clamp 成“无需伸缩”。
        if (desiredRaw == current) {
            DecisionResult r = DecisionResult.noop(w.getId(), "SCALE",
                    RejectReason.NO_SCALE_NEEDED, basis, now, w.getStateVersion());
            decisionLog.append(r);
            return r;
        }

        boolean up = desiredRaw > current;
        long lastAction = up ? w.getLastScaleUpAt() : w.getLastScaleDownAt();
        long cooldown = up ? p.scaleUpCooldownMillis() : p.scaleDownCooldownMillis();
        if (now - lastAction < cooldown) {
            DecisionResult r = DecisionResult.rejected(w.getId(), "SCALE", null,
                    RejectReason.SCALE_IN_COOLDOWN,
                    basis + "; " + (up ? "scale-up" : "scale-down")
                            + " cooldown remaining " + (cooldown - (now - lastAction)) + "ms",
                    now, w.getStateVersion());
            decisionLog.append(r);
            return r;
        }

        if (up && current >= p.maxReplicas()) {
            DecisionResult r = DecisionResult.rejected(w.getId(), "SCALE", null,
                    RejectReason.SCALE_AT_MAX_REPLICAS, basis, now, w.getStateVersion());
            decisionLog.append(r);
            return r;
        }
        if (!up && current <= p.minReplicas()) {
            DecisionResult r = DecisionResult.rejected(w.getId(), "SCALE", null,
                    RejectReason.SCALE_AT_MIN_REPLICAS, basis, now, w.getStateVersion());
            decisionLog.append(r);
            return r;
        }

        int target = desired;
        if (up) {
            for (int i = current; i < target; i++) {
                w.addInstance(InstanceStatus.HEALTHY);
            }
        } else {
            // 原子性前置校验：可安全终止的健康实例数量不足时完全不迁移，
            // 绝不允许“终止了一部分再失败”导致副本计数与实例集合不一致。
            int toRemove = current - target;
            List<String> healthyTargets = new ArrayList<>();
            for (var e : w.instanceStatusSnapshot().entrySet()) {
                if (e.getValue() == InstanceStatus.HEALTHY) {
                    healthyTargets.add(e.getKey());
                }
            }
            if (healthyTargets.size() < toRemove) {
                DecisionResult r = DecisionResult.rejected(w.getId(), "SCALE", null,
                        RejectReason.SCALE_IN_PROGRESS,
                        basis + "; need " + toRemove + " healthy instance(s) for termination but only "
                                + healthyTargets.size() + " available (others are repairing/terminal)",
                        now, w.getStateVersion());
                decisionLog.append(r);
                return r;
            }
            // 确定性地终止插入顺序最后的健康实例（见 Workload#chooseScaleDownTarget 的选择规则）。
            for (int k = 0; k < toRemove; k++) {
                String victim = healthyTargets.get(healthyTargets.size() - 1 - k);
                w.changeStatus(victim, InstanceStatus.TERMINATED);
            }
        }
        w.recordScale(now, up, target);
        DecisionResult ok = DecisionResult.scale(w.getId(), current, target,
                now, w.getStateVersion());
        DecisionResult withBasis = new DecisionResult(true, w.getId(), "SCALE", current, target,
                null, null, RejectReason.OK, basis, now, w.getStateVersion());
        decisionLog.append(withBasis);
        return ok;
    }

    private static double avg(double load, int replicas) {
        return replicas <= 0 ? load : load / replicas;
    }

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    /**
     * 带阈值滞后的确定性目标副本计算（纯函数，<b>不做</b> min/max 边界裁剪）：
     * <pre>
     *   avg = load / max(current, 1)
     *   avg &gt; upThreshold   ->  ceil(load / upThreshold)
     *   avg &lt; downThreshold ->  max(1, floor(load / downThreshold))（滞后带内保持不变）
     * </pre>
     * 边界（副本数上下限、零负载）由编排层结合冷却与拒绝原因处理。
     */
    public static int computeDesiredReplicasRaw(int current, double observedLoad,
                                                ScalingPolicy policy) {
        int effective = Math.max(current, 1);
        double avg = observedLoad / effective;
        int desired = current;
        if (avg > policy.scaleUpThreshold()) {
            desired = (int) Math.ceil(observedLoad / policy.scaleUpThreshold());
        } else if (avg < policy.scaleDownThreshold()) {
            if (policy.scaleDownThreshold() == 0.0d) {
                desired = 1;
            } else {
                desired = (int) Math.floor(observedLoad / policy.scaleDownThreshold());
            }
            desired = Math.max(1, desired);
        }
        return desired;
    }

    /** 带 [minReplicas, maxReplicas] 裁剪的目标副本（纯函数）。 */
    public static int computeDesiredReplicas(int current, double observedLoad,
                                             ScalingPolicy policy) {
        return clamp(computeDesiredReplicasRaw(current, observedLoad, policy),
                policy.minReplicas(), policy.maxReplicas());
    }
}
