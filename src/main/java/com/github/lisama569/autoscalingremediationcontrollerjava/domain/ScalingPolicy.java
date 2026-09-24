package com.github.lisama569.autoscalingremediationcontrollerjava.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * 工作负载伸缩策略（不可变值对象）。
 *
 * <ul>
 *   <li>minReplicas/maxReplicas：副本数上下界（包含）。</li>
 *   <li>scaleUpThreshold/scaleDownThreshold：每副本平均负载的扩容/缩容阈值，
 *       且 scaleDownThreshold &lt; scaleUpThreshold，形成阈值滞后带，避免抖动。</li>
 *   <li>scaleUpCooldownMillis/scaleDownCooldownMillis：同方向两次伸缩的最小间隔；
 *       冷却期内请求得到确定性的 SCALE_IN_COOLDOWN 拒绝。</li>
 *   <li>remediationEnabled：是否允许自愈；禁止时显式返回 REMEDIATION_DISABLED。</li>
 *   <li>maxConcurrentRepairs：单工作负载并发修复上限。</li>
 *   <li>failureBudget + failureBudgetWindowMillis：滑动窗口内允许的修复失败次数；
 *       耗尽后新修复一律 REM_FAILURE_BUDGET_EXHAUSTED，窗口滑过后自动恢复。</li>
 *   <li>repairTimeoutMillis：单次修复执行的截止时间，超时以 REPAIR_TIMEOUT 终态结束。</li>
 *   <li>repairCooldownMillis：同一实例两次修复入队的最小间隔。</li>
 *   <li>unhealthyThreshold：实例连续被观测为不健康的次数门槛，用于吸收健康抖动。</li>
 *   <li>observationMaxAgeMillis：最新负载观测的最大允许年龄；超过即视为过期，
 *       伸缩判定以 OBSERVATION_STALE 拒绝（0 表示不检查年龄）。</li>
 * </ul>
 */
public record ScalingPolicy(
        long version,
        int minReplicas,
        int maxReplicas,
        double scaleUpThreshold,
        double scaleDownThreshold,
        long scaleUpCooldownMillis,
        long scaleDownCooldownMillis,
        boolean remediationEnabled,
        int maxConcurrentRepairs,
        int failureBudget,
        long failureBudgetWindowMillis,
        long repairTimeoutMillis,
        long repairCooldownMillis,
        int unhealthyThreshold,
        long observationMaxAgeMillis
) {
    /**
     * 返回所有非法项的说明；为空表示合法。调用方必须以该结果为准，不得猜测或静默修正。
     */
    public List<String> validate() {
        List<String> errors = new ArrayList<>();
        if (version < 0) {
            errors.add("version must be non-negative");
        }
        if (minReplicas < 0) {
            errors.add("minReplicas must be non-negative");
        }
        if (maxReplicas < minReplicas) {
            errors.add("maxReplicas must be >= minReplicas");
        }
        if (!(scaleUpThreshold > 0)) {
            errors.add("scaleUpThreshold must be positive");
        }
        if (!(scaleDownThreshold >= 0)) {
            errors.add("scaleDownThreshold must be non-negative");
        }
        if (scaleDownThreshold >= scaleUpThreshold) {
            errors.add("scaleDownThreshold must be < scaleUpThreshold (hysteresis band required)");
        }
        if (scaleUpCooldownMillis < 0 || scaleDownCooldownMillis < 0) {
            errors.add("cooldown values must be non-negative");
        }
        if (maxConcurrentRepairs <= 0) {
            errors.add("maxConcurrentRepairs must be positive");
        }
        if (failureBudget < 0) {
            errors.add("failureBudget must be non-negative");
        }
        if (failureBudgetWindowMillis < 0) {
            errors.add("failureBudgetWindowMillis must be non-negative");
        }
        if (repairTimeoutMillis <= 0) {
            errors.add("repairTimeoutMillis must be positive");
        }
        if (repairCooldownMillis < 0) {
            errors.add("repairCooldownMillis must be non-negative");
        }
        if (unhealthyThreshold <= 0) {
            errors.add("unhealthyThreshold must be positive");
        }
        if (observationMaxAgeMillis < 0) {
            errors.add("observationMaxAgeMillis must be non-negative (0 disables staleness check)");
        }
        return List.copyOf(errors);
    }

    public boolean isValid() {
        return validate().isEmpty();
    }
}
