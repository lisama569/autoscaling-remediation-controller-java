package com.github.lisama569.autoscalingremediationcontrollerjava.tests;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.PolicyUpdateResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 伸缩语义与确定性：
 * 非法策略/缺少观测必须给可区分拒绝原因；阈值滞后、稳定次数、冷却、版本更新期间行为确定；
 * 同一输入序列重复执行得到一致结论。
 */
class AutoscaleDeterminismTest {

    private void logInputs(TestHarness h, String title) {
        System.out.println("==== " + title + " ====");
    }

    @Test
    void rejectsWithDistinguishableReasons() {
        TestHarness h = new TestHarness();
        String wl = "wl-reject";
        h.controller.registerWorkload(wl, 1);

        // 未注册工作负载
        ScaleDecision unknown = h.controller.reconcileScale("does-not-exist");
        assertEquals(ScaleOutcome.REJECTED, unknown.outcome());
        assertEquals(RejectionReason.WORKLOAD_NOT_REGISTERED, unknown.rejectionReason());
        System.out.println("[reject] unknown workload -> " + unknown.rejectionReason());

        // 缺少策略
        ScaleDecision noPolicy = h.controller.reconcileScale(wl);
        assertEquals(RejectionReason.MISSING_POLICY, noPolicy.rejectionReason());
        System.out.println("[reject] no policy -> " + noPolicy.rejectionReason());

        // 非法策略：min > max
        ScalePolicy bad = new ScalePolicy(1L, 5, 2, 0.8, 0.5, 2, 500L,
                true, 1, 2, 10_000L, 300L, 1_000L);
        PolicyUpdateResult badResult = h.controller.updatePolicy(wl, bad);
        assertEquals(RejectionReason.INVALID_POLICY, badResult.rejectionReason());
        System.out.println("[reject] invalid policy -> " + badResult.rejectionReason()
                + " detail=" + badResult.rationale());

        // 合法策略后缺少负载观测
        h.controller.updatePolicy(wl, TestHarness.defaultPolicy());
        ScaleDecision noLoad = h.controller.reconcileScale(wl);
        assertEquals(RejectionReason.MISSING_LOAD_OBSERVATION, noLoad.rejectionReason());
        System.out.println("[reject] no load -> " + noLoad.rejectionReason());

        // 过期观测：在接入时即以可区分原因拒绝，状态不被覆盖（仍无观测）
        long now = h.clock.nowMillis();
        var staleAccept = h.controller.observeLoad(new LoadObservation(wl, 1L, now - 30_001L, 0.95));
        assertEquals(false, staleAccept.accepted());
        assertEquals(RejectionReason.STALE_LOAD_OBSERVATION, staleAccept.rejectionReason());
        System.out.println("[reject] stale load ingest -> " + staleAccept.rejectionReason()
                + " rationale=" + staleAccept.rationale());
        ScaleDecision stillMissing = h.controller.reconcileScale(wl);
        assertEquals(RejectionReason.MISSING_LOAD_OBSERVATION, stillMissing.rejectionReason());

        // 未来时间戳的观测同样拒绝
        var futureAccept = h.controller.observeLoad(new LoadObservation(wl, 2L, now + 10L, 0.95));
        assertEquals(RejectionReason.STALE_LOAD_OBSERVATION, futureAccept.rejectionReason());
        h.controller.eventLog().dump();
    }

    @Test
    void hysteresisStabilizationCooldownAreDeterministic() {
        logInputs(new TestHarness(), "hysteresis + stabilization + cooldown");
        TestHarness h = new TestHarness();
        String wl = "wl-scale";
        h.controller.registerWorkload(wl, 2);
        h.controller.updatePolicy(wl, TestHarness.defaultPolicy()); // min=1,max=5, rounds=2, cooldown=500

        // 滞后带内：NO_CHANGE 且计数清零
        pushLoad(h, wl, 1L, 0.6);
        assertEquals(ScaleOutcome.NO_CHANGE, h.controller.reconcileScale(wl).outcome());

        // 第一次超阈值：streak=1 不动作
        pushLoad(h, wl, 2L, 0.9);
        ScaleDecision first = h.controller.reconcileScale(wl);
        assertEquals(ScaleOutcome.NO_CHANGE, first.outcome());
        System.out.println("[streak] upStreak=1 -> " + first.outcome() + " : " + first.rationale());

        // 第二次超阈值：扩容 2 -> 3
        pushLoad(h, wl, 3L, 0.9);
        ScaleDecision scaled = h.controller.reconcileScale(wl);
        assertEquals(ScaleOutcome.SCALED_UP, scaled.outcome());
        assertEquals(3, scaled.targetReplicas());
        System.out.println("[scale] " + scaled.previousReplicas() + "->" + scaled.targetReplicas()
                + " : " + scaled.rationale());

        // 紧接着再来两次高负载：第二次应被冷却拦截
        pushLoad(h, wl, 4L, 0.9);
        h.controller.reconcileScale(wl);
        pushLoad(h, wl, 5L, 0.9);
        ScaleDecision cooling = h.controller.reconcileScale(wl);
        assertEquals(ScaleOutcome.IN_COOLDOWN, cooling.outcome());
        System.out.println("[cooldown] -> " + cooling.rationale());

        // 冷却结束后，冷却期间保留的 streak 使下一次高负载立即动作：扩容 3 -> 4
        h.advance(501L);
        pushLoad(h, wl, 6L, 0.9);
        ScaleDecision afterCooldown = h.controller.reconcileScale(wl);
        assertEquals(ScaleOutcome.SCALED_UP, afterCooldown.outcome());
        assertEquals(4, afterCooldown.targetReplicas());
        System.out.println("[scale-after-cooldown] -> " + afterCooldown.targetReplicas()
                + " : " + afterCooldown.rationale());

        // 到达上限 5 后 NO_CHANGE
        h.advance(501L);
        pushLoad(h, wl, 8L, 0.9);
        h.controller.reconcileScale(wl);
        pushLoad(h, wl, 9L, 0.9);
        h.controller.reconcileScale(wl); // 4 -> 5
        h.advance(501L);
        pushLoad(h, wl, 10L, 0.9);
        h.controller.reconcileScale(wl);
        pushLoad(h, wl, 11L, 0.9);
        ScaleDecision atMax = h.controller.reconcileScale(wl);
        assertEquals(ScaleOutcome.NO_CHANGE, atMax.outcome());
        assertEquals(5, h.controller.snapshot(wl).desiredReplicas());
        System.out.println("[ceiling] at max -> " + atMax.rationale());
        h.controller.eventLog().dump();
    }

    @Test
    void policyVersionRollbackRejectedAndDoesNotOverwrite() {
        TestHarness h = new TestHarness();
        String wl = "wl-policy";
        h.controller.registerWorkload(wl, 1);
        h.controller.updatePolicy(wl, TestHarness.defaultPolicy()); // v1

        ScalePolicy v1Rollback = new ScalePolicy(1L, 2, 8, 0.8, 0.5, 1, 0L,
                true, 1, 2, 10_000L, 300L, 1_000L);
        PolicyUpdateResult conflict = h.controller.updatePolicy(wl, v1Rollback);
        assertEquals(RejectionReason.POLICY_VERSION_CONFLICT, conflict.rejectionReason());
        // 状态未被覆盖：min 仍是 1
        assertEquals(1, h.controller.snapshot(wl).policy().minReplicas());
        System.out.println("[policy] rollback rejected -> " + conflict.rationale());

        ScalePolicy v2 = new ScalePolicy(2L, 2, 8, 0.8, 0.5, 1, 0L,
                true, 1, 2, 10_000L, 300L, 1_000L);
        PolicyUpdateResult ok = h.controller.updatePolicy(wl, v2);
        assertEquals(true, ok.accepted());
        // 更新后 desired 被钳制进新区间：1 -> 2 且实例立即落地
        WorkloadState state = h.controller.snapshot(wl);
        assertEquals(2, state.desiredReplicas());
        assertEquals(2, state.instances().size());
        System.out.println("[policy] v2 applied, desired clamped -> " + state.desiredReplicas()
                + " instances=" + state.instances().size());
    }

    @Test
    void sameInputSequenceYieldsSameResults() {
        List<ScaleOutcome> run1 = runScenario();
        List<ScaleOutcome> run2 = runScenario();
        assertEquals(run1, run2);
        System.out.println("[determinism] repeated run outcomes=" + run2);
    }

    private List<ScaleOutcome> runScenario() {
        TestHarness h = new TestHarness();
        String wl = "wl-determinism";
        h.controller.registerWorkload(wl, 1);
        h.controller.updatePolicy(wl, TestHarness.defaultPolicy());
        List<ScaleOutcome> outcomes = new ArrayList<>();
        double[] ratios = {0.9, 0.9, 0.6, 0.2, 0.2, 0.9};
        long v = 1;
        for (double r : ratios) {
            pushLoad(h, wl, v++, r);
            outcomes.add(h.controller.reconcileScale(wl).outcome());
            h.advance(501L);
        }
        return outcomes;
    }

    private void pushLoad(TestHarness h, String wl, long version, double ratio) {
        h.controller.observeLoad(new LoadObservation(wl, version, h.clock.nowMillis(), ratio));
    }
}
