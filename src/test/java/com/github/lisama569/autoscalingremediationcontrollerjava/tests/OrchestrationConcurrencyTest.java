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
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.RepairOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Workload;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发编排与重复/乱序事件测试：
 * 任意线程交错下同一工作负载不得超额扩容、不得重复修复；
 * 过期/重复观测不得覆盖状态；固定输入序列两次执行的最终状态完全一致。
 */
class OrchestrationConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationConcurrencyTest.class);

    private static ScalingPolicy policy(long version) {
        return new ScalingPolicy(version, 2, 5, 1.0d, 0.4d,
                1000L, 1000L, true, 2, 3, 10_000L, 2000L, 500L, 2, 0L);
    }

    private record Harness(VirtualClock clock, WorkloadRegistry registry,
                           Autoscaler autoscaler, RemediationManager remediation,
                           SimulationEngine engine, DecisionLog decisionLog) {
    }

    private Harness newHarness() {
        VirtualClock clock = new VirtualClock(0L);
        DecisionLog dl = new DecisionLog();
        PolicyValidator validator = new PolicyValidator();
        WorkloadRegistry registry = new WorkloadRegistry(clock, validator, dl);
        Autoscaler autoscaler = new Autoscaler(registry, clock, dl);
        RemediationManager remediation = new RemediationManager(registry, clock, dl);
        SimulationEngine engine = new SimulationEngine(registry, remediation, clock, dl);
        return new Harness(clock, registry, autoscaler, remediation, engine, dl);
    }

    @Test
    void concurrentScaleTriggersProduceExactlyOneScale() throws Exception {
        Harness h = newHarness();
        h.registry.register("w", policy(1), 2);
        h.registry.receiveObservation("w", "src", 1L, 6.0d);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<DecisionResult> results = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    barrier.await();
                    results.add(h.autoscaler.evaluate("w"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long acceptedScales = results.stream()
                .filter(r -> r.accepted() && "SCALE".equals(r.kind())
                        && r.toReplicas() != null)
                .count();
        long cooldowns = results.stream()
                .filter(r -> r.reason() == RejectReason.SCALE_IN_COOLDOWN).count();
        log.info("[并发] {} 个伸缩触发：有效扩容={}，冷却拒绝={}，最终副本数={}",
                threads, acceptedScales, cooldowns, h.registry.get("w").getCurrentReplicas());
        assertEquals(1, acceptedScales, "并发交错下只能有一次有效扩容");
        assertEquals(5, h.registry.get("w").getCurrentReplicas(), "不得超额扩容");
        assertEquals(threads - 1, cooldowns + results.stream()
                .filter(r -> r.reason() == RejectReason.SCALE_AT_MAX_REPLICAS).count());
    }

    @Test
    void duplicateAndOutOfOrderObservationsNeverOverwriteState() {
        Harness h = newHarness();
        h.registry.register("w", policy(1), 2);

        DecisionResult first = h.registry.receiveObservation("w", "src", 5L, 3.0d);
        DecisionResult dup = h.registry.receiveObservation("w", "src", 5L, 9.0d);
        DecisionResult stale = h.registry.receiveObservation("w", "src", 4L, 9.0d);
        DecisionResult newer = h.registry.receiveObservation("w", "src", 6L, 9.0d);
        // 另一个来源的旧序号不影响本来源。
        DecisionResult other = h.registry.receiveObservation("w", "src2", 1L, 1.0d);

        log.info("[观测] 首次={} 重复={} 乱序回退={} 更新={} 第二来源={}",
                first.reason(), dup.reason(), stale.reason(), newer.reason(), other.reason());
        assertTrue(first.accepted());
        assertEquals(RejectReason.OBSERVATION_DUPLICATE, dup.reason());
        assertEquals(RejectReason.OBSERVATION_STALE, stale.reason());
        assertTrue(newer.accepted());
        assertTrue(other.accepted());
        // 同 observedAt 时按 sourceId 字典序确定性选取：src2 > src。
        assertEquals("src2", h.registry.get("w").getLatestObservation().sourceId());
        assertEquals(1.0d, h.registry.get("w").getLatestObservation().observedLoad(), 1e-9);
        // 但 src 来源自身的最新序号仍是 6，乱序/重复没有污染它。
        h.clock.advanceMillis(1L);
        h.registry.receiveObservation("w", "src", 7L, 2.0d);
        assertEquals(2.0d, h.registry.get("w").getLatestObservation().observedLoad(), 1e-9);
        DecisionResult replayOld = h.registry.receiveObservation("w", "src", 6L, 99.0d);
        assertEquals(RejectReason.OBSERVATION_STALE, replayOld.reason());
    }

    @Test
    void concurrentRemediationTriggersProduceSingleTask() throws Exception {
        Harness h = newHarness();
        h.registry.register("w", policy(1), 3);
        Workload w = h.registry.get("w");
        String iid;
        synchronized (w.monitor()) {
            iid = w.instanceStatusSnapshot().keySet().iterator().next();
        }
        h.registry.reportHealth("w", iid, false);
        h.registry.reportHealth("w", iid, false);

        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<DecisionResult> results = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    barrier.await();
                    results.add(h.remediation.trigger("w", iid, "CONCURRENT_TEST"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long started = results.stream().filter(r -> r.accepted()
                && r.taskId() != null && r.detail() != null
                && r.detail().contains("repair started")).count();
        long rejected = results.stream().filter(r ->
                r.reason() == RejectReason.REM_ALREADY_IN_PROGRESS
                        || r.reason() == RejectReason.REM_ALREADY_QUEUED).count();
        log.info("[并发] {} 个修复触发：启动={}，幂等拒绝={}，activeRepairs={}",
                threads, started, rejected, h.remediation.activeRepairCount("w"));
        assertEquals(1, started, "同一实例只能启动一个修复任务");
        assertEquals(1, h.remediation.activeRepairCount("w"), "不得重复修复");
        assertEquals(threads - 1, rejected);
        assertFalse(h.remediation.trigger("w", iid, "again").accepted());
    }

    @Test
    void identicalInputSequencesConvergeToIdenticalStates() {
        Map<String, Object> run1 = replayOnce(11L);
        Map<String, Object> run2 = replayOnce(11L);
        log.info("[复现] run1 最终状态={}", run1);
        log.info("[复现] run2 最终状态={}", run2);
        assertEquals(run1, run2, "相同输入序列必须收敛到相同最终状态与决策结论");

        // 乱序到达的观测序列也必须可复现。
        Map<String, Object> shuffled1 = replayOnce(29L);
        Map<String, Object> shuffled2 = replayOnce(29L);
        assertEquals(shuffled1, shuffled2);
    }

    /** 固定脚本：注册 -> 观测 -> 并发扫描 -> tick 推进 -> 修复成功，返回最终状态指纹。 */
    @SuppressWarnings("unchecked")
    private Map<String, Object> replayOnce(long seed) {
        Harness h = newHarness();
        h.registry.register("w", policy(1), 3);
        Workload w = h.registry.get("w");
        List<String> ids;
        synchronized (w.monitor()) {
            ids = List.copyOf(w.instanceStatusSnapshot().keySet());
        }
        String a = ids.get(0);
        String b = ids.get(1);

        h.remediation.setBehavior((wid, iid, attempt, now) ->
                iid.equals(a) ? RepairOutcome.REPAIRED : RepairOutcome.PENDING);

        // 观测以固定乱序序列到达（始终相同），序号防护保证最终最新值确定。
        long[] seq = seed % 2 == 0 ? new long[]{3L, 1L, 2L, 4L} : new long[]{4L, 2L, 1L, 3L};
        double[] loads = {8.0d, 1.0d, 4.0d, 6.0d};
        for (int i = 0; i < seq.length; i++) {
            h.registry.receiveObservation("w", "src", seq[i], loads[i]);
        }

        for (int i = 0; i < 3; i++) {
            h.registry.reportHealth("w", a, false);
            h.registry.reportHealth("w", b, false);
        }
        h.remediation.scan("w");
        h.engine.tick(100);
        h.clock.advanceMillis(500);
        h.engine.tick(1);
        h.autoscaler.evaluate("w");

        List<String> decisionTrace = h.decisionLog.entries().stream()
                .map(r -> r.kind() + ":" + r.reason()
                        + (r.fromReplicas() == null ? "" : ":" + r.fromReplicas() + "->" + r.toReplicas())
                        + (r.instanceId() == null ? "" : ":" + r.instanceId()))
                .collect(Collectors.toList());
        return Map.of(
                "replicas", w.getCurrentReplicas(),
                "instances", w.instanceStatusSnapshot(),
                "activeRepairs", h.remediation.activeRepairCount("w"),
                "queuedRepairs", h.remediation.queuedRepairCount("w"),
                "remainingBudget", h.remediation.remainingFailureBudget("w"),
                "decisions", decisionTrace);
    }
}
