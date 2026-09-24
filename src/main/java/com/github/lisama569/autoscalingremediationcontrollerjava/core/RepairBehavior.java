package com.github.lisama569.autoscalingremediationcontrollerjava.core;

import com.github.lisama569.autoscalingremediationcontrollerjava.domain.RepairOutcome;

/**
 * 修复动作的可脚本化行为接口。默认全部修复成功；测试可注入失败/超时/替换脚本，
 * 从而让“健康抖动、修复失败、预算耗尽、恢复”场景完全可复现。
 */
@FunctionalInterface
public interface RepairBehavior {
    RepairOutcome outcomeFor(String workloadId, String instanceId, int attempt, long nowMillis);

    RepairBehavior ALWAYS_REPAIRED = (w, i, a, t) -> RepairOutcome.REPAIRED;
}
