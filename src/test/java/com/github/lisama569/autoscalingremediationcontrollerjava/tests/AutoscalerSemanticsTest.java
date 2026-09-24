package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.VirtualClock;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionLog;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.PolicyValidator;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.RejectReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.WorkloadRegistry;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 策略语义与可拒绝原因测试：非法策略、阈值滞后、冷却、观测缺失/过期、策略版本回退，
 * 以及纯函数在相同输入下的可复现性。
 */
class AutoscalerSemanticsTest {

    private static final Logger log = LoggerFactory.getLogger(AutoscalerSemanticsTest.class);

    private static ScalingPolicy policy(long version, long maxAge) {
        return new ScalingPolicy(version, 2, 5, 1.0d, 0.4d,
                1000L, 1000L, true, 2, 2, 10_000L, 2000L, 500L, 2, maxAge);
    }

    private record Harness(VirtualClock clock, WorkloadRegistry registry, Autoscaler autoscaler,
                           DecisionLog decisionLog) {
    }

    private Harness newHarness() {
        VirtualClock clock = new VirtualClock(0L);
        DecisionLog dl = new DecisionLog();
        PolicyValidator validator = new PolicyValidator();
        WorkloadRegistry registry = new WorkloadRegistry(clock, validator, dl);
        Autoscaler autoscaler = new Autoscaler(registry, clock, dl);
        return new Harness(clock, registry, autoscaler, dl);
    }

    @Test
    void invalidPoliciesAreRejectedWithDistinctMessages() {
        ScalingPolicy bad1 = new ScalingPolicy(1, 5, 2, 1.0, 0.4,
                0, 0, true, 1, 1, 100, 100, 0, 1, 0);
        ScalingPolicy bad2 = new ScalingPolicy(1, 1, 5, 0.4, 0.9,
                0, 0, true, 1, 1, 100, 100, 0, 1, 0);
        ScalingPolicy bad3 = new ScalingPolicy(1, 1, 5, 1.0, 0.4,
                -1, 0, true, 0, -1, 100, 0, -1, 0, -1);
        List<String> e1 = new PolicyValidator().validate(bad1);
        List<String> e2 = new PolicyValidator().validate(bad2);
        List<String> e3 = new PolicyValidator().validate(bad3);
        log.info("[输入] 非法策略1错误项={}", e1);
        log.info("[输入] 非法策略2错误项={}", e2);
        log.info("[输入] 非法策略3错误项={}", e3);
        assertFalse(e1.isEmpty());
        assertFalse(e2.isEmpty());
        assertTrue(e3.size() >= 6, "应报告全部非法项，实际: " + e3);

        Harness h = newHarness();
        DecisionResult r = h.registry.register("w", bad1, 2);
        assertFalse(r.accepted());
        assertEquals(RejectReason.POLICY_INVALID, r.reason());
    }

    @Test
    void missingObservationGivesDistinctRejection() {
        Harness h = newHarness();
        h.registry.register("w", policy(1, 0), 2);
        DecisionResult r = h.autoscaler.evaluate("w");
        log.info("[判定] 无观测 evaluate -> accepted={} reason={} detail='{}'",
                r.accepted(), r.reason(), r.detail());
        assertFalse(r.accepted());
        assertEquals(RejectReason.OBSERVATION_MISSING, r.reason());

        // 明确上报“无负载数据”同样不得给出模糊结果。
        h.registry.receiveObservation("w", "src", 1L, null);
        DecisionResult r2 = h.autoscaler.evaluate("w");
        log.info("[判定] 观测无负载值 -> reason={} detail='{}'", r2.reason(), r2.detail());
        assertFalse(r2.accepted());
        assertEquals(RejectReason.OBSERVATION_MISSING, r2.reason());
    }

    @Test
    void staleObservationIsRejectedAfterMaxAge() {
        Harness h = newHarness();
        h.registry.register("w", policy(1, 5000L), 2);
        h.registry.receiveObservation("w", "src", 1L, 8.0d);
        h.clock.advanceMillis(6000L);
        DecisionResult r = h.autoscaler.evaluate("w");
        log.info("[迁移] 观测超龄 evaluate -> reason={} detail='{}'", r.reason(), r.detail());
        assertFalse(r.accepted());
        assertEquals(RejectReason.OBSERVATION_STALE, r.reason());
    }

    @Test
    void hysteresisBandAndCooldownAreDeterministic() {
        Harness h = newHarness();
        h.registry.register("w", policy(1, 0), 2);

        h.registry.receiveObservation("w", "src", 1L, 6.0d);
        DecisionResult scale1 = h.autoscaler.evaluate("w"); // 2 -> 5 (max)
        log.info("[迁移] 高负载扩容 {} -> {}, reason={}", scale1.fromReplicas(),
                scale1.toReplicas(), scale1.reason());
        assertTrue(scale1.accepted());
        assertEquals(5, scale1.toReplicas());

        // 冷却期内重复触发：确定性拒绝，不产生额外迁移。
        h.clock.advanceMillis(100L);
        h.registry.receiveObservation("w", "src", 2L, 6.0d);
        DecisionResult cooling = h.autoscaler.evaluate("w");
        log.info("[判定] 冷却中重复触发 -> reason={} detail='{}'",
                cooling.reason(), cooling.detail());
        assertFalse(cooling.accepted());
        assertEquals(RejectReason.SCALE_IN_COOLDOWN, cooling.reason());
        assertEquals(5, h.registry.get("w").getCurrentReplicas());

        // 冷却过后但仍在上限。
        h.clock.advanceMillis(1000L);
        h.registry.receiveObservation("w", "src", 3L, 6.0d);
        DecisionResult atMax = h.autoscaler.evaluate("w");
        assertEquals(RejectReason.SCALE_AT_MAX_REPLICAS, atMax.reason());

        // 负载落到滞后带内（avg=1.0）：不伸缩。
        h.clock.advanceMillis(1000L);
        h.registry.receiveObservation("w", "src", 4L, 5.0d);
        DecisionResult band = h.autoscaler.evaluate("w");
        log.info("[判定] 滞后带内 avg=1.0 -> reason={}", band.reason());
        assertEquals(RejectReason.NO_SCALE_NEEDED, band.reason());
        assertEquals(5, h.registry.get("w").getCurrentReplicas());

        // 低负载缩容：floor(1.0/0.4)=2 -> min 2，5 -> 2 一步到位。
        h.clock.advanceMillis(1000L);
        h.registry.receiveObservation("w", "src", 5L, 1.0d);
        DecisionResult down = h.autoscaler.evaluate("w");
        log.info("[迁移] 低负载缩容 {} -> {}", down.fromReplicas(), down.toReplicas());
        assertTrue(down.accepted());
        assertEquals(2, down.toReplicas());

        h.clock.advanceMillis(1000L);
        h.registry.receiveObservation("w", "src", 6L, 0.1d);
        DecisionResult atMin = h.autoscaler.evaluate("w");
        assertEquals(RejectReason.SCALE_AT_MIN_REPLICAS, atMin.reason());
    }

    @Test
    void policyVersionRollbackIsRejected() {
        Harness h = newHarness();
        h.registry.register("w", policy(1, 0), 2);
        DecisionResult ok = h.registry.updatePolicy("w", policy(2, 0));
        assertTrue(ok.accepted());
        DecisionResult rollback = h.registry.updatePolicy("w", policy(2, 0)); // 相同版本
        DecisionResult older = h.registry.updatePolicy("w", policy(1, 0));    // 回退
        log.info("[判定] 策略同版本更新 reason={}; 版本回退 reason={}",
                rollback.reason(), older.reason());
        assertEquals(RejectReason.POLICY_VERSION_STALE, rollback.reason());
        assertEquals(RejectReason.POLICY_VERSION_STALE, older.reason());
        assertEquals(2L, h.registry.get("w").getPolicy().version());
    }

    @Test
    void desiredReplicasPureFunctionIsReproducible() {
        ScalingPolicy p = policy(1, 0);
        for (int run = 0; run < 3; run++) {
            int a = Autoscaler.computeDesiredReplicas(2, 6.0d, p);
            int b = Autoscaler.computeDesiredReplicas(2, 6.0d, p);
            int c = Autoscaler.computeDesiredReplicas(2, 6.0d, p);
            assertEquals(a, b);
            assertEquals(b, c);
            assertEquals(5, a, "ceil(6/1)=6 被 maxReplicas=5 裁剪");
            assertEquals(6, Autoscaler.computeDesiredReplicasRaw(2, 6.0d, p),
                    "原始目标在裁剪前应为 6");
            int low = Autoscaler.computeDesiredReplicas(4, 0.5d, p);
            assertEquals(2, low, "floor(0.5/0.4)=1 -> min 2");
            log.info("[复现] run={} desired(2,6.0)={} raw=6 desired(4,0.5)={}", run, a, low);
        }
    }
}
