package com.github.lisama569.autoscalingremediationcontrollerjava.infra;

/**
 * 任务启动时确定的修复执行计划（耗时在启动时已知，因此超时判定确定）。
 *
 * @param success        计划结果是否成功
 * @param timedOut       是否按超时终止（脚本化，模拟修复卡死）
 * @param durationMillis 计划耗时
 */
public record RepairPlan(boolean success, boolean timedOut, long durationMillis) {
}
