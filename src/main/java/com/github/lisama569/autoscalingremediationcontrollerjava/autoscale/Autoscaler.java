package com.github.lisama569.autoscalingremediationcontrollerjava.autoscale;

import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityGate;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityReservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadAggregate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 目标副本数计算器（在调用方持有工作负载串行锁时执行）。
 *
 * <p>语义（完全确定性）：
 * <ol>
 *   <li>{@code ratio > scaleUpThreshold}：upStreak+1；{@code ratio <= scaleDownThreshold}：downStreak+1；
 *       位于滞后带 (down, up] 内：两个计数清零，NO_CHANGE。</li>
 *   <li>连续满足 stabilizationRounds 次才产生一个方向的伸缩意图。</li>
 *   <li>距上次伸缩不足 cooldownMillis：返回 IN_COOLDOWN，保留已积累的 streak（冷却结束后即可动作）。</li>
 *   <li>每次决策最多调整 1 个副本；达到 max/min 边界返回 NO_CHANGE 并说明。</li>
 *   <li>实际发生伸缩后清零两个 streak 并刷新 lastScaleAtMillis。</li>
 * </ol>
 * 缺少策略/观测或观测过期，返回带具体 {@link RejectionReason} 的 REJECTED，绝不猜测。
 */
@Component
public class Autoscaler {

    private final CapacityGate capacityGate;

    public Autoscaler() {
        this(null);
    }

    @Autowired
    public Autoscaler(CapacityGate capacityGate) {
        this.capacityGate = capacityGate;
    }

    public ScaleDecision evaluate(WorkloadAggregate agg, long nowMillis, long maxObservationAgeMillis) {
        String wl = agg.getWorkloadId();
        ScalePolicy policy = agg.getPolicy();
        if (policy == null) {
            return ScaleDecision.rejected(wl, RejectionReason.MISSING_POLICY,
                    "no policy attached to workload " + wl, Long.MIN_VALUE, agg.getLatestLoadVersion());
        }
        LoadObservation load = agg.getLatestLoad();
        if (load == null) {
            return ScaleDecision.rejected(wl, RejectionReason.MISSING_LOAD_OBSERVATION,
                    "no load observation for workload " + wl, policy.version(), Long.MIN_VALUE);
        }
        long age = nowMillis - load.observedAtMillis();
        if (age > maxObservationAgeMillis) {
            return ScaleDecision.rejected(wl, RejectionReason.STALE_LOAD_OBSERVATION,
                    "load v" + load.version() + " age " + age + "ms > maxAge "
                            + maxObservationAgeMillis + "ms", policy.version(), load.version());
        }

        int previous = agg.getDesiredReplicas();
        double ratio = load.loadRatio();
        boolean wantsUp = ratio > policy.scaleUpThreshold();
        boolean wantsDown = ratio <= policy.scaleDownThreshold();

        // 一旦最新观测不再指向扩容（滞后带/明确缩容），此前“未满足的扩容需求”即作废：
        // 退出 FIFO 队列并把目标收回到当前副本，避免 applyDesiredReplicas 又把它重新入队。
        if (agg.isPendingCapacity() && !wantsUp && capacityGate != null) {
            capacityGate.cancelWait(wl);
            agg.setPendingCapacity(false);
            agg.setDesiredReplicas(agg.activeInstanceCount());
        }

        if (wantsUp) {
            agg.setUpStreak(agg.getUpStreak() + 1);
            agg.setDownStreak(0);
        } else if (wantsDown) {
            agg.setDownStreak(agg.getDownStreak() + 1);
            agg.setUpStreak(0);
        } else {
            agg.setUpStreak(0);
            agg.setDownStreak(0);
            return decision(wl, ScaleOutcome.NO_CHANGE, previous, previous, policy, load,
                    "ratio " + ratio + " in hysteresis band (" + policy.scaleDownThreshold()
                            + ", " + policy.scaleUpThreshold() + "]; streaks reset");
        }

        boolean upReady = agg.getUpStreak() >= policy.stabilizationRounds();
        boolean downReady = agg.getDownStreak() >= policy.stabilizationRounds();

        if (!upReady && !downReady) {
            return decision(wl, ScaleOutcome.NO_CHANGE, previous, previous, policy, load,
                    "streak not stabilized: up=" + agg.getUpStreak() + ", down=" + agg.getDownStreak()
                            + ", need " + policy.stabilizationRounds());
        }

        if (agg.getLastScaleAtMillis() != Long.MIN_VALUE) {
            long elapsed = nowMillis - agg.getLastScaleAtMillis();
            if (elapsed < policy.cooldownMillis()) {
                return decision(wl, ScaleOutcome.IN_COOLDOWN, previous, previous, policy, load,
                        "cooldown active: " + (policy.cooldownMillis() - elapsed) + "ms remaining; "
                                + "up=" + agg.getUpStreak() + ", down=" + agg.getDownStreak());
            }
        }

        // 同一条观测（重复事件/重复调度）至多触发一次伸缩，防止重复扩容。
        // 例外：已在容量队列中等待的工作负载，其意向尚未真正落地，重复评估应继续返回
        // PENDING_CAPACITY（刷新等待），而不是被短路成 NO_CHANGE。
        if (!agg.isPendingCapacity() && load.version() == agg.getLastActedObservationVersion()) {
            return decision(wl, ScaleOutcome.NO_CHANGE, previous, previous, policy, load,
                    "observation v" + load.version() + " already acted upon; duplicate event ignored");
        }

        if (upReady) {
            if (previous >= policy.maxReplicas()) {
                return decision(wl, ScaleOutcome.NO_CHANGE, previous, previous, policy, load,
                        "scale-up desired but already at maxReplicas=" + policy.maxReplicas());
            }
            // 共享容量检查：意向合法、稳定与冷却都已通过，这里只差总盘额度
            if (capacityGate != null) {
                CapacityReservation reservation = capacityGate.requestScaleUpUnit(wl);
                if (!reservation.granted()) {
                    // 记录“未满足需求”：目标 +1（受 maxReplicas 钳制），但不占用额度、不创建实例。
                    // 后续 drain 时按 desired - 已授予额度 逐份补足；lastScaleAt 也推进，
                    // 因此等待期间不占冷却，额度一到即可落地。
                    int wanted = Math.min(policy.maxReplicas(), previous + 1);
                    agg.setDesiredReplicas(wanted);
                    agg.setLastScaleAtMillis(nowMillis);
                    agg.setLastActedObservationVersion(load.version());
                    agg.setUpStreak(0);
                    agg.setDownStreak(0);
                    agg.setPendingCapacity(true);
                    ScaleDecision pending = new ScaleDecision(wl, ScaleOutcome.PENDING_CAPACITY,
                            previous, wanted, policy.version(), load.version(),
                            RejectionReason.SHARED_CAPACITY_EXHAUSTED,
                            "scale-up desired but shared capacity exhausted: used="
                                    + reservation.usedCapacity() + "/" + reservation.totalCapacity()
                                    + "; enqueued FIFO at position " + reservation.queuePosition()
                                    + "; desired=" + wanted + " will be granted on release");
                    return pending;
                }
            }
            int target = previous + 1;
            agg.setDesiredReplicas(target);
            agg.setLastScaleAtMillis(nowMillis);
            agg.setLastActedObservationVersion(load.version());
            agg.setUpStreak(0);
            agg.setDownStreak(0);
            agg.setPendingCapacity(false);
            return decision(wl, ScaleOutcome.SCALED_UP, previous, target, policy, load,
                    "ratio " + ratio + " > upThreshold " + policy.scaleUpThreshold()
                            + "; replicas " + previous + " -> " + target
                            + (capacityGate != null ? "; capacity reserved" : ""));
        }
        // 走到缩容分支说明已不再需要扩容：若此前在排队，先取消等待，避免额度账与意向不一致
        if (agg.isPendingCapacity() && capacityGate != null) {
            capacityGate.cancelWait(wl);
            agg.setPendingCapacity(false);
        }
        if (previous <= policy.minReplicas()) {
            return decision(wl, ScaleOutcome.NO_CHANGE, previous, previous, policy, load,
                    "scale-down desired but already at minReplicas=" + policy.minReplicas());
        }
        int target = previous - 1;
        agg.setDesiredReplicas(target);
        agg.setLastScaleAtMillis(nowMillis);
        agg.setLastActedObservationVersion(load.version());
        agg.setUpStreak(0);
        agg.setDownStreak(0);
        return decision(wl, ScaleOutcome.SCALED_DOWN, previous, target, policy, load,
                "ratio " + ratio + " <= downThreshold " + policy.scaleDownThreshold()
                        + "; replicas " + previous + " -> " + target);
    }

    private static ScaleDecision decision(String wl, ScaleOutcome outcome, int previous, int target,
                                          ScalePolicy policy, LoadObservation load, String rationale) {
        return new ScaleDecision(wl, outcome, previous, target,
                policy.version(), load.version(), null, rationale);
    }
}
