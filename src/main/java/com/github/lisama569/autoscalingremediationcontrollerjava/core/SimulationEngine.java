package com.github.lisama569.autoscalingremediationcontrollerjava.core;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.Clock;
import com.github.lisama569.autoscalingremediationcontrollerjava.clock.VirtualClock;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 确定性进程内模拟引擎。
 *
 * <p>一个 tick 的语义固定为：可选地把虚拟时钟向前推进 deltaMillis（时间不可回退），
 * 随后让修复管理器推进全部工作负载的执行中任务与排队任务。
 * 伸缩与健康观测均由外部在 tick 之间显式注入，因此任何输入序列都对应唯一的
 * 状态迁移序列，重复执行得到一致的最终状态与决策结论。</p>
 */
@Component
public final class SimulationEngine {

    private final WorkloadRegistry registry;
    private final RemediationManager remediationManager;
    private final Clock clock;
    private final DecisionLog decisionLog;

    public SimulationEngine(WorkloadRegistry registry, RemediationManager remediationManager,
                            Clock clock, DecisionLog decisionLog) {
        this.registry = registry;
        this.remediationManager = remediationManager;
        this.clock = clock;
        this.decisionLog = decisionLog;
    }

    /** 不推进时间，仅推进一次修复循环（例如脚本瞬时成功时）。 */
    public List<DecisionResult> pump() {
        return remediationManager.pump();
    }

    /**
     * 推进虚拟时钟 deltaMillis，再推进修复循环。
     * 只有装配的时钟是 VirtualClock 时才允许推进，墙钟环境直接 pump。
     */
    public List<DecisionResult> tick(long deltaMillis) {
        if (clock instanceof VirtualClock vc) {
            vc.advanceMillis(deltaMillis);
        }
        return remediationManager.pump();
    }
}
