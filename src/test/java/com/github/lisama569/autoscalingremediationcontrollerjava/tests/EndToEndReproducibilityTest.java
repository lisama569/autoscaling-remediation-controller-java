package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 端到端可复现：同样的事件序列（含负载变化、健康抖动、修复失败与恢复）在全新的
 * 确定性环境中执行两次，最终状态快照与全部决策日志必须完全一致。
 */
class EndToEndReproducibilityTest {

    /** 归一化快照：去除 UUID 等非确定性标识，只保留决定系统行为的字段。 */
    private record NormalizedState(int desired, int instanceCount,
                                   List<String> instanceStatuses,
                                   List<RemediationStatus> taskStatuses,
                                   List<String> taskModes,
                                   List<Integer> taskAttempts,
                                   List<String> taskDetails) {
    }

    private NormalizedState normalize(WorkloadState s) {
        List<String> statuses = s.instances().stream()
                .map(i -> i.getId().replaceAll("-[0-9a-f]{8}-[0-9a-f-]+", "-UUID")
                        .replaceAll("rm-UUID", "rm-UUID") + ":" + i.getStatus())
                .sorted().toList();
        List<RemediationStatus> ts = s.remediations().stream().map(t -> t.getStatus()).toList();
        List<String> modes = s.remediations().stream().map(t -> t.getMode().name()).toList();
        List<Integer> attempts = s.remediations().stream().map(t -> t.getAttempt()).toList();
        List<String> details = s.remediations().stream()
                .map(t -> (t.getDetail() == null ? "" :
                        t.getDetail().replaceAll("-[0-9a-f]{8}-[0-9a-f-]+", "-UUID")))
                .toList();
        return new NormalizedState(s.desiredReplicas(), s.instances().size(),
                statuses, ts, modes, attempts, details);
    }

    /** 归一化日志：抹掉时间戳中的固定起点差异与 UUID。 */
    private List<String> normalizeLog(List<String> entries) {
        List<String> out = new ArrayList<>();
        for (String e : entries) {
            String x = e.replaceAll("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "UUID");
            out.add(x);
        }
        return out;
    }

    private NormalizedState runScenario(boolean dumpLog) {
        TestHarness h = new TestHarness();
        String wl = "shop";
        h.controller.registerWorkload(wl, 2);
        ScalePolicy p = new ScalePolicy(1L, 1, 6, 0.8, 0.5, 2, 400L,
                true, 2, 2, 5_000L, 200L, 800L);
        h.controller.updatePolicy(wl, p);

        String i1 = wl + "-inst-1";
        String i2 = wl + "-inst-2";

        // 1) 流量上升两轮 -> 扩容
        load(h, wl, 1L, 0.9); h.controller.reconcileScale(wl);
        load(h, wl, 2L, 0.9); h.controller.reconcileScale(wl); // 2 -> 3
        h.advance(401L);
        load(h, wl, 3L, 0.9); h.controller.reconcileScale(wl);
        load(h, wl, 4L, 0.9); h.controller.reconcileScale(wl); // 3 -> 4

        // 2) 实例 1 不健康，首次修复失败
        h.infra.scriptRepair(i1, false, false, 100L);
        health(h, wl, i1, 1L, false);
        h.advance(100L);
        h.controller.tickRemediations();

        // 3) 修复后立刻又收到不健康（抖动），受实例冷却/预算约束
        health(h, wl, i1, 2L, false);

        // 4) 冷却结束，第二次修复走替换，成功
        h.advance(201L);
        h.infra.scriptRepair(i1, true, false, 100L);
        h.controller.requestRemediation(wl, i1);
        h.advance(100L);
        h.controller.tickRemediations();

        // 5) 实例 2 修复超时（800ms）
        h.infra.scriptRepair(i2, true, false, 2_000L);
        health(h, wl, i2, 1L, false);
        h.advance(800L);
        h.controller.tickRemediations(); // 预算此时耗尽（i1 失败 + i2 超时）

        // 6) 预算窗口过后恢复：i2 第二次（替换）成功
        h.advance(5_001L);
        h.infra.scriptRepair(i2, true, false, 100L);
        h.controller.requestRemediation(wl, i2);
        h.advance(100L);
        h.controller.tickRemediations();

        // 7) 流量回落到滞后带 -> 不缩容；继续低负载两轮 -> 缩容一次
        h.advance(401L);
        load(h, wl, 5L, 0.6); h.controller.reconcileScale(wl);
        load(h, wl, 6L, 0.4); h.controller.reconcileScale(wl);
        h.advance(401L);
        load(h, wl, 7L, 0.4); h.controller.reconcileScale(wl); // 4 -> 3

        // 8) 健康抖动：新实例自发恢复健康观测（验证状态机）
        health(h, wl, i2 + "-repl", 1L, true);

        if (dumpLog) {
            h.controller.eventLog().dump();
        }
        NormalizedState normalized = normalize(h.controller.snapshot(wl));
        // 把日志归一化后挂到细节外（用一次强校验保证日志也确定）
        lastLog = normalizeLog(h.controller.eventLog().entries());
        return normalized;
    }

    private List<String> lastLog = new ArrayList<>();

    @Test
    void repeatedRunsProduceIdenticalFinalStateAndLogs() {
        NormalizedState runA = runScenario(false);
        List<String> logA = new ArrayList<>(lastLog);

        NormalizedState runB = runScenario(true);
        List<String> logB = new ArrayList<>(lastLog);

        assertEquals(runA, runB, "final state must be identical across runs");
        assertEquals(logA, logB, "decision logs must be identical across runs");
        assertNotEquals(0, runA.taskStatuses().size());
        // 终态不变量：没有活动任务残留
        assertEquals(0, runA.taskStatuses().stream()
                .filter(s -> s == RemediationStatus.PENDING || s == RemediationStatus.RUNNING).count());
        // 所有存活实例都是 READY
        assertEquals(runA.instanceCount(),
                runA.instanceStatuses().stream().filter(s -> s.endsWith(":" + InstanceStatus.READY)).count());
        System.out.println("[reproduce] final=" + runA);
        System.out.println("[reproduce] logLines=" + logB.size());
    }

    private void load(TestHarness h, String wl, long v, double ratio) {
        h.controller.observeLoad(new LoadObservation(wl, v, h.clock.nowMillis(), ratio));
    }

    private void health(TestHarness h, String wl, String id, long v, boolean healthy) {
        h.controller.observeHealth(new HealthObservation(wl, id, v, h.clock.nowMillis(), healthy));
    }
}
