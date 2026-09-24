package com.github.lisama569.autoscalingremediationcontrollerjava.autoscale;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadAggregate;
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

    public Autoscaler() {
        // 判定时间由调用方显式传入，保证可测试
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

        // 同一条观测（重复事件/重复调度）至多触发一次伸缩，防止重复扩容
        if (load.version() == agg.getLastActedObservationVersion()) {
            return decision(wl, ScaleOutcome.NO_CHANGE, previous, previous, policy, load,
                    "observation v" + load.version() + " already acted upon; duplicate event ignored");
        }

        if (upReady) {
            if (previous >= policy.maxReplicas()) {
                return decision(wl, ScaleOutcome.NO_CHANGE, previous, previous, policy, load,
                        "scale-up desired but already at maxReplicas=" + policy.maxReplicas());
            }
            int target = previous + 1;
            agg.setDesiredReplicas(target);
            agg.setLastScaleAtMillis(nowMillis);
            agg.setLastActedObservationVersion(load.version());
            agg.setUpStreak(0);
            agg.setDownStreak(0);
            return decision(wl, ScaleOutcome.SCALED_UP, previous, target, policy, load,
                    "ratio " + ratio + " > upThreshold " + policy.scaleUpThreshold()
                            + "; replicas " + previous + " -> " + target);
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
