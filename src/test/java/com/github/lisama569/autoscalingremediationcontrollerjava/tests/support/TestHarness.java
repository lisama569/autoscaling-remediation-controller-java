package com.github.lisama569.autoscalingremediationcontrollerjava.tests.support;

import com.github.lisama569.autoscalingremediationcontrollerjava.autoscale.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityPool;
import com.github.lisama569.autoscalingremediationcontrollerjava.infra.SimulatedInfrastructure;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.orchestration.Controller;
import com.github.lisama569.autoscalingremediationcontrollerjava.orchestration.EventLog;
import com.github.lisama569.autoscalingremediationcontrollerjava.remediation.RemediationExecutor;
import com.github.lisama569.autoscalingremediationcontrollerjava.remediation.RemediationService;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.ObservationStore;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadRegistry;
import com.github.lisama569.autoscalingremediationcontrollerjava.time.ManualClock;

/** 纯手动装配：固定时钟、进程内存储，保证测试确定且可复现。 */
public final class TestHarness {

    public final ManualClock clock;
    public final WorkloadRegistry registry;
    public final ObservationStore observationStore;
    public final Autoscaler autoscaler;
    public final SimulatedInfrastructure infra;
    public final RemediationExecutor remediationExecutor;
    public final RemediationService remediationService;
    public final EventLog eventLog;
    public final CapacityPool capacityPool;
    public final Controller controller;

    public TestHarness() {
        this(30_000L, 1_000);
    }

    public TestHarness(long maxObservationAgeMillis) {
        this(maxObservationAgeMillis, 1_000);
    }

    /**
     * @param totalGlobalCapacity 全局共享实例总盘子；默认 1000 足够大，使原有单工作负载用例
     *                            行为完全不变；多工作负载容量协作用例显式传小值。
     */
    public TestHarness(long maxObservationAgeMillis, int totalGlobalCapacity) {
        clock = new ManualClock(1_000L);
        registry = new WorkloadRegistry();
        observationStore = new ObservationStore(registry);
        autoscaler = new Autoscaler();
        infra = new SimulatedInfrastructure();
        eventLog = new EventLog();
        remediationExecutor = new RemediationExecutor(infra, clock, eventLog);
        remediationService = new RemediationService(registry, remediationExecutor, clock, eventLog);
        capacityPool = new CapacityPool(totalGlobalCapacity);
        controller = new Controller(registry, observationStore, autoscaler, remediationService,
                remediationExecutor, infra, clock, eventLog, maxObservationAgeMillis, capacityPool);
    }

    public void advance(long millis) {
        clock.advance(millis);
    }

    /** 标准合法策略，测试可按需覆盖个别字段。 */
    public static ScalePolicy defaultPolicy() {
        return new ScalePolicy(1L, 1, 5, 0.8d, 0.5d, 2, 500L,
                true, 1, 2, 10_000L, 300L, 1_000L);
    }
}
