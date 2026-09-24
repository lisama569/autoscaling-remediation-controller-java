package com.github.lisama569.autoscalingremediationcontrollerjava.model;

/**
 * 伸缩策略（不可变）。version 必须单调递增；旧版本不得覆盖新版本。
 *
 * @param version            策略版本号，从 1 开始单调递增
 * @param minReplicas        最小副本数
 * @param maxReplicas        最大副本数
 * @param scaleUpThreshold   扩容阈值（观测负载/容量比严格大于该值触发扩容）
 * @param scaleDownThreshold 缩容阈值（小于等于该值触发缩容）
 * @param stabilizationRounds 连续满足次数，防止抖动
 * @param cooldownMillis     同一工作负载两次伸缩之间的冷却时间
 * @param remediationEnabled 是否允许自愈修复
 * @param remediationConcurrency 单工作负载同时进行的修复任务上限
 * @param failureBudget      每个预算窗口允许的连续/累计失败次数
 * @param failureBudgetWindowMillis 预算窗口长度
 * @param remediationCooldownMillis 单次修复失败后再次修复该实例的冷却时间
 * @param remediationTimeoutMillis 单次修复超时时间
 */
public record ScalePolicy(
        long version,
        int minReplicas,
        int maxReplicas,
        double scaleUpThreshold,
        double scaleDownThreshold,
        int stabilizationRounds,
        long cooldownMillis,
        boolean remediationEnabled,
        int remediationConcurrency,
        int failureBudget,
        long failureBudgetWindowMillis,
        long remediationCooldownMillis,
        long remediationTimeoutMillis
) {
}
