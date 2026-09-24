package com.github.lisama569.autoscalingremediationcontrollerjava.demo;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.VirtualClock;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionLog;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.RemediationManager;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.SimulationEngine;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.WorkloadRegistry;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.RepairOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 本地确定性演示（--controller.demo=true 启用，不启动 Web 也可运行：见 README）。
 *
 * <p>脚本：注册 -> 高负载两次扩容 -> 冷却期内被拒绝 -> 到达上限 ->
 * 单帧健康抖动被门槛吸收 -> 连续不健康触发修复 -> 一个实例失败重试后恢复、
 * 另一个实例超时耗尽失败预算 -> 滑动窗口恢复后重新修复成功 -> 低负载缩容。</p>
 */
@Component
@ConditionalOnProperty(name = "controller.demo", havingValue = "true")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);
    private static final String WID = "demo-svc";

    private final WorkloadRegistry registry;
    private final Autoscaler autoscaler;
    private final RemediationManager remediationManager;
    private final SimulationEngine simulationEngine;
    private final VirtualClock clock;
    private final DecisionLog decisionLog;

    public DemoRunner(WorkloadRegistry registry, Autoscaler autoscaler,
                      RemediationManager remediationManager, SimulationEngine simulationEngine,
                      VirtualClock clock, DecisionLog decisionLog) {
        this.registry = registry;
        this.autoscaler = autoscaler;
        this.remediationManager = remediationManager;
        this.simulationEngine = simulationEngine;
        this.clock = clock;
        this.decisionLog = decisionLog;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("==== DEMO START ====");
        ScalingPolicy policy = new ScalingPolicy(
                1L, 2, 5, 1.0d, 0.4d,
                1000L, 1000L, true, 2, 2, 10_000L, 2000L, 500L, 2, 5_000L);
        registry.register(WID, policy, 2);

        Workload w = registry.get(WID);
        String inst1;
        String inst2;
        List<String> ids;
        synchronized (w.monitor()) {
            ids = List.copyOf(w.instanceStatusSnapshot().keySet());
        }
        inst1 = ids.get(0);
        inst2 = ids.get(1);

        // 修复脚本：inst1 在 3100ms 之前 PENDING（模拟耗时，首个任务会在 2000ms 截止后超时），
        // 之后重新触发即成功；inst2 始终失败（maxAttempts=1，一次失败即终止）。
        remediationManager.setBehavior((wid, iid, attempt, now) -> {
            if (iid.equals(inst1)) {
                return now < 3100L ? RepairOutcome.PENDING : RepairOutcome.REPAIRED;
            }
            return RepairOutcome.FAILED;
        });
        remediationManager.setMaxAttempts(1);

        section("high load scales out");
        registry.receiveObservation(WID, "metrics-a", 1L, 6.0d);
        autoscaler.evaluate(WID);           // 2 -> ceil(6/1)=6 -> clamp 5
        clock.advanceMillis(100);
        registry.receiveObservation(WID, "metrics-a", 2L, 6.0d);
        autoscaler.evaluate(WID);           // 冷却中：SCALE_IN_COOLDOWN
        clock.advanceMillis(1000);
        registry.receiveObservation(WID, "metrics-a", 3L, 6.0d);
        autoscaler.evaluate(WID);           // 已在上限：SCALE_AT_MAX_REPLICAS

        section("single health jitter is absorbed, persistent unhealthy triggers repair");
        registry.reportHealth(WID, inst1, false);   // streak 1，不修复
        remediationManager.scan(WID);                // 无合格实例
        registry.reportHealth(WID, inst1, false);   // streak 2
        registry.reportHealth(WID, inst2, false);
        registry.reportHealth(WID, inst2, false);
        remediationManager.scan(WID);                // 两个任务：并发上限 2，均直接执行

        section("repair in progress: pending then budget exhaustion");
        simulationEngine.tick(500);    // inst1 PENDING；inst2 FAILED -> 尝试耗尽（maxAttempts=1）
        log.info("remaining failure budget = {}", remediationManager.remainingFailureBudget(WID));
        clock.advanceMillis(1500);
        simulationEngine.tick(1);      // inst1 超过截止 -> REPAIR_TIMEOUT（第二次失败，预算耗尽）
        log.info("remaining failure budget = {}", remediationManager.remainingFailureBudget(WID));

        section("budget recovery after sliding window");
        clock.advanceMillis(10_500L);  // 滑出失败窗口
        registry.reportHealth(WID, inst1, false);
        registry.reportHealth(WID, inst1, false);
        remediationManager.trigger(WID, inst1, "DEMO"); // 预算恢复，重新修复
        simulationEngine.tick(1);                       // 修复成功恢复

        section("low load scales in after cooldown");
        clock.advanceMillis(1000);
        registry.receiveObservation(WID, "metrics-a", 4L, 0.5d);
        autoscaler.evaluate(WID);       // avg 低 -> 缩容
        clock.advanceMillis(1000);
        registry.receiveObservation(WID, "metrics-a", 5L, 0.0d);
        autoscaler.evaluate(WID);       // 继续缩到 minReplicas=2

        log.info("==== DEMO FINAL STATE ====");
        synchronized (w.monitor()) {
            w.instanceStatusSnapshot().forEach((iid, st) ->
                    log.info("instance {} status={}", iid, st));
        }
        log.info("total decisions recorded = {}", decisionLog.entries().size());
        log.info("==== DEMO END ====");
    }

    private void section(String title) {
        log.info("---- DEMO STEP: {} (now={}) ----", title, clock.nowMillis());
    }
}
