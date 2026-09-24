package com.github.lisama569.autoscalingremediationcontrollerjava.domain;

/**
 * 一次修复执行轮询的脚本化结果（仅用于进程内确定性模拟）。
 */
public enum RepairOutcome {
    /** 修复尚在进行，本轮不产生终态（用于确定性模拟修复耗时）。 */
    PENDING,
    /** 修复成功，实例恢复健康。 */
    REPAIRED,
    /** 修复成功并以新实例替换旧实例。 */
    REPLACED,
    /** 修复脚本失败；未达最大尝试次数时原地重试。 */
    FAILED,
    /** 修复超过截止时间，以超时终态结束。 */
    TIMEOUT
}
