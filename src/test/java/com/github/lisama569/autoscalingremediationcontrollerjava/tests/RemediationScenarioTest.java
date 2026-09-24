package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.VirtualClock;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionLog;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.PolicyValidator;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.RejectReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.RemediationManager;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.SimulationEngine;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.WorkloadRegistry;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.RepairOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Workload;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 自愈编排场景测试：健康抖动吸收、并发上限排队与按序晋升、失败预算耗尽、
 * 滑动窗口恢复、修复超时、尝试次数耗尽、策略禁止、修复冷却、恢复后无重复任务。
 */
class RemediationScenarioTest {

    private static final Logger log = LoggerFactory.getLogger(RemediationScenarioTest.class);

    private record Harness(VirtualClock clock, WorkloadRegistry registry,
                           Autoscaler autoscaler, RemediationManager remediation,
                           SimulationEngine engine, DecisionLog decisionLog) {
    }

    private static ScalingPolicy policy(boolean enabled, int concurrency, int budget) {
        return policy(1L, enabled, concurrency, budget);
    }

    private static ScalingPolicy policy(long version, boolean enabled, int concurrency, int budget) {
        return new ScalingPolicy(version, 2, 8, 1.0d, 0.4d,
                1000L, 1000L, enabled, concurrency, budget, 10_000L,
                2000L, 500L, 2, 0L);
    }

    private Harness newHarness(boolean enabled, int concurrency, int budget) {
        VirtualClock clock = new VirtualClock(0L);
        DecisionLog dl = new DecisionLog();
        PolicyValidator validator = new PolicyValidator();
        WorkloadRegistry registry = new WorkloadRegistry(clock, validator, dl);
        Autoscaler autoscaler = new Autoscaler(registry, clock, dl);
        RemediationManager remediation = new RemediationManager(registry, clock, dl);
        SimulationEngine engine = new SimulationEngine(registry, remediation, clock, dl);
        registry.register("w", policy(enabled, concurrency, budget), 4);
        return new Harness(clock, registry, autoscaler, remediation, engine, dl);
    }

    private List<String> instanceIds(Harness h) {
        synchronized (h.registry.get("w").monitor()) {
            return List.copyOf(h.registry.get("w").instanceStatusSnapshot().keySet());
        }
    }

    private void markUnhealthy(Harness h, String iid, int frames) {
        for (int i = 0; i < frames; i++) {
            h.registry.reportHealth("w", iid, false);
        }
    }

    @Test
    void singleFrameJitterIsAbsorbedAndStreakResetsOnRecovery() {
        Harness h = newHarness(true, 2, 3);
        String a = instanceIds(h).get(0);

        h.registry.reportHealth("w", a, false); // streak 1 < 门槛 2
        List<DecisionResult> scan1 = h.remediation.scan("w");
        log.info("[抖动] 单帧不健康后扫描：{}", scan1.stream().map(DecisionResult::reason).toList());
        assertTrue(scan1.isEmpty(), "单帧抖动不得触发修复");

        h.registry.reportHealth("w", a, true);  // 恢复，streak 清零
        h.registry.reportHealth("w", a, false); // streak 重新计为 1
        List<DecisionResult> scan2 = h.remediation.scan("w");
        assertTrue(scan2.isEmpty(), "健康帧必须打断连续不健康计数");

        markUnhealthy(h, a, 2);                 // streak 2，达到门槛
        DecisionResult triggered = h.remediation.scan("w").get(0);
        log.info("[抖动] 连续两帧不健康后触发：accepted={} detail='{}'",
                triggered.accepted(), triggered.detail());
        assertTrue(triggered.accepted());
        assertEquals(1, h.remediation.activeRepairCount("w"));
    }

    @Test
    void concurrencyLimitQueuesAndPromotesInOrder() {
        Harness h = newHarness(true, 1, 5);
        List<String> ids = instanceIds(h);
        String a = ids.get(0);
        String b = ids.get(1);
        String c = ids.get(2);
        markUnhealthy(h, a, 2);
        markUnhealthy(h, b, 2);
        markUnhealthy(h, c, 2);

        List<DecisionResult> decisions = h.remediation.scan("w");
        log.info("[并发上限] 三个不健康实例的判定：{}",
                decisions.stream().map(DecisionResult::reason).toList());
        assertEquals(1, h.remediation.activeRepairCount("w"));
        assertEquals(2, h.remediation.queuedRepairCount("w"));

        // 第一个任务始终 PENDING -> 超过 2000ms 截止而超时失败，腾出槽位，晋升下一个。
        h.remediation.setBehavior((w, iid, attempt, now) ->
                iid.equals(a) ? RepairOutcome.REPAIRED : RepairOutcome.REPAIRED);
        h.engine.tick(1); // a 立即修复成功，b 晋升并执行成功，c 随之晋升成功
        log.info("[并发上限] 连续修复后 active={} queued={}",
                h.remediation.activeRepairCount("w"), h.remediation.queuedRepairCount("w"));
        assertEquals(0, h.remediation.activeRepairCount("w"));
        assertEquals(0, h.remediation.queuedRepairCount("w"));
        Workload wl = h.registry.get("w");
        assertEquals(InstanceStatus.HEALTHY, wl.getInstance(a).getStatus());
        assertEquals(InstanceStatus.HEALTHY, wl.getInstance(b).getStatus());
        assertEquals(InstanceStatus.HEALTHY, wl.getInstance(c).getStatus());
    }

    @Test
    void failureBudgetExhaustsThenRecoversAfterSlidingWindow() {
        Harness h = newHarness(true, 2, 2);
        h.remediation.setMaxAttempts(1);
        h.remediation.setBehavior((w, iid, at, now) -> RepairOutcome.FAILED);
        List<String> ids = instanceIds(h);
        String a = ids.get(0);
        String b = ids.get(1);
        String c = ids.get(2);

        markUnhealthy(h, a, 2);
        markUnhealthy(h, b, 2);
        h.remediation.scan("w");
        List<DecisionResult> terminals = h.engine.tick(1);
        long attemptsExhausted = terminals.stream()
                .filter(r -> r.reason() == RejectReason.REPAIR_ATTEMPTS_EXHAUSTED).count();
        log.info("[预算] 两个任务终态={}，剩余预算={}",
                terminals.stream().map(DecisionResult::reason).toList(),
                h.remediation.remainingFailureBudget("w"));
        assertEquals(2, attemptsExhausted);
        assertEquals(0, h.remediation.remainingFailureBudget("w"));

        // 预算耗尽后，新的修复请求必须被显式拒绝，且不得入队。
        markUnhealthy(h, c, 2);
        DecisionResult rejected = h.remediation.trigger("w", c, "TEST");
        log.info("[预算] 耗尽后新请求 reason={} queued={}", rejected.reason(),
                h.remediation.queuedRepairCount("w"));
        assertEquals(RejectReason.REM_FAILURE_BUDGET_EXHAUSTED, rejected.reason());
        assertEquals(0, h.remediation.queuedRepairCount("w"));
        assertEquals(InstanceStatus.UNHEALTHY, h.registry.get("w").getInstance(c).getStatus());

        // 滑出失败窗口后预算恢复，c 可修复成功；终态失败的 a 在冷却后也可重新修复。
        h.clock.advanceMillis(10_500L);
        assertEquals(2, h.remediation.remainingFailureBudget("w"));
        h.remediation.setBehavior((w, iid, at, now) -> RepairOutcome.REPAIRED);
        DecisionResult recovered = h.remediation.trigger("w", c, "TEST");
        assertTrue(recovered.accepted(), "预算恢复后应允许修复");
        h.engine.tick(1);
        assertEquals(InstanceStatus.HEALTHY, h.registry.get("w").getInstance(c).getStatus());
        log.info("[预算] 滑动窗口恢复后 c 状态={}",
                h.registry.get("w").getInstance(c).getStatus());
    }

    @Test
    void timeoutProducesDistinctTerminalState() {
        Harness h = newHarness(true, 1, 3);
        String a = instanceIds(h).get(0);
        h.remediation.setBehavior((w, iid, at, now) -> RepairOutcome.PENDING);
        markUnhealthy(h, a, 2);
        h.remediation.trigger("w", a, "TEST");

        List<DecisionResult> beforeDeadline = h.engine.tick(100);
        assertEquals(1, h.remediation.activeRepairCount("w"), "截止前任务继续执行");
        assertEquals(InstanceStatus.REPAIRING, h.registry.get("w").getInstance(a).getStatus());

        h.clock.advanceMillis(2000L);
        List<DecisionResult> afterDeadline = h.engine.tick(1);
        DecisionResult terminal = afterDeadline.get(0);
        log.info("[超时] 截止后终态 reason={} detail='{}'", terminal.reason(), terminal.detail());
        assertEquals(RejectReason.REPAIR_TIMEOUT, terminal.reason());
        assertFalse(terminal.accepted());
        assertEquals(0, h.remediation.activeRepairCount("w"));
        assertEquals(InstanceStatus.REMEDIATION_FAILED,
                h.registry.get("w").getInstance(a).getStatus());
    }

    @Test
    void disabledPolicyAndCooldownAndDuplicateProtection() {
        Harness h = newHarness(false, 1, 3);
        String a = instanceIds(h).get(0);
        markUnhealthy(h, a, 2);
        DecisionResult disabled = h.remediation.trigger("w", a, "TEST");
        assertEquals(RejectReason.REMEDIATION_DISABLED, disabled.reason());
        log.info("[禁止] 策略关闭自愈 -> reason={}", disabled.reason());

        // 换用启用策略（版本 2）。
        DecisionResult updated = h.registry.updatePolicy("w", policy(2L, true, 1, 3));
        assertTrue(updated.accepted());
        DecisionResult first = h.remediation.trigger("w", a, "TEST");
        assertTrue(first.accepted());
        DecisionResult duplicate = h.remediation.trigger("w", a, "TEST");
        assertEquals(RejectReason.REM_ALREADY_IN_PROGRESS, duplicate.reason());
        log.info("[重复] 执行中重复触发 -> reason={}", duplicate.reason());

        h.remediation.setBehavior((w, iid, at, now) -> RepairOutcome.REPAIRED);
        h.engine.tick(1);
        // 实例已恢复健康：重复触发必须是无实例可修，而不是新建任务。
        DecisionResult healthyNow = h.remediation.trigger("w", a, "TEST");
        assertEquals(RejectReason.REM_NO_UNHEALTHY_INSTANCE, healthyNow.reason());

        // 再次连续不健康，但仍在修复冷却期内：可区分拒绝。
        markUnhealthy(h, a, 2);
        DecisionResult cooling = h.remediation.trigger("w", a, "TEST");
        assertEquals(RejectReason.REM_IN_REMEDIATION_COOLDOWN, cooling.reason());
        log.info("[冷却] 冷却期内重新触发 -> reason={} detail='{}'",
                cooling.reason(), cooling.detail());
        assertEquals(0, h.remediation.queuedRepairCount("w"), "冷却拒绝不得残留任务");

        // 冷却过后可以重新修复，且仍然只有一个任务。
        h.clock.advanceMillis(501L);
        DecisionResult again = h.remediation.trigger("w", a, "TEST");
        assertTrue(again.accepted());
        assertEquals(1, h.remediation.activeRepairCount("w"));
        h.engine.tick(1);
        assertEquals(0, h.remediation.activeRepairCount("w"), "恢复后不得残留重复任务");
    }

    @Test
    void scaleDownIsRejectedAtomicallyWhenHealthyInstancesAreBusy() {
        Harness h = newHarness(true, 4, 5);
        h.remediation.setBehavior((w, iid, at, now) -> RepairOutcome.PENDING);
        List<String> ids = instanceIds(h);
        // 让 3 个实例进入修复中，只剩 1 个健康实例。
        for (int i = 0; i < 3; i++) {
            markUnhealthy(h, ids.get(i), 2);
            h.remediation.trigger("w", ids.get(i), "TEST");
        }
        assertEquals(3, h.remediation.activeRepairCount("w"));
        h.registry.receiveObservation("w", "s", 1L, 0.1d);
        h.clock.advanceMillis(2000L); // 越过伸缩冷却（无历史伸缩，冷却本就满足）
        DecisionResult r = h.autoscaler.evaluate("w");
        log.info("[原子性] 修复占用下缩容 reason={} detail='{}'", r.reason(), r.detail());
        assertEquals(RejectReason.SCALE_IN_PROGRESS, r.reason());
        assertEquals(4, h.registry.get("w").getCurrentReplicas(), "拒绝后副本数不得变化");
        assertEquals(4, h.registry.get("w").instanceStatusSnapshot().size(),
                "拒绝后不得有任何实例被终止");
    }
}
