package com.github.lisama569.autoscalingremediationcontrollerjava.orchestration;

import com.github.lisama569.autoscalingremediationcontrollerjava.autoscale.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.autoscale.PolicyValidator;
import com.github.lisama569.autoscalingremediationcontrollerjava.infra.SimulatedInfrastructure;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ObservationAcceptResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.PolicyUpdateResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationRequestOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationTask;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.remediation.RemediationExecutor;
import com.github.lisama569.autoscalingremediationcontrollerjava.remediation.RemediationService;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.ObservationStore;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadAggregate;
import com.github.lisama569.autoscalingremediationcontrollerjava.store.WorkloadRegistry;
import com.github.lisama569.autoscalingremediationcontrollerjava.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 编排门面。
 *
 * <p><b>并发模型：</b>每个 workloadId 一把锁（{@link #locks}）。同一工作负载的策略写入、
 * 观测写入、伸缩与修复推进全部串行化，因此任意线程交错下不会出现超额扩容、漏扩容或重复修复；
 * 不同工作负载之间完全并行。
 *
 * <p><b>版本规则：</b>策略与观测均要求版本严格单调递增，回退/重复版本被拒绝且不覆盖状态。
 */
@Component
public class Controller {

    private final WorkloadRegistry registry;
    private final ObservationStore observationStore;
    private final Autoscaler autoscaler;
    private final RemediationService remediationService;
    private final RemediationExecutor remediationExecutor;
    private final SimulatedInfrastructure infra;
    private final Clock clock;
    private final EventLog eventLog;
    private final long maxObservationAgeMillis;

    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    public Controller(WorkloadRegistry registry,
                      ObservationStore observationStore,
                      Autoscaler autoscaler,
                      RemediationService remediationService,
                      RemediationExecutor remediationExecutor,
                      SimulatedInfrastructure infra,
                      Clock clock,
                      EventLog eventLog,
                      @Value("${arc.max-observation-age-millis:30000}") long maxObservationAgeMillis) {
        this.registry = registry;
        this.observationStore = observationStore;
        this.autoscaler = autoscaler;
        this.remediationService = remediationService;
        this.remediationExecutor = remediationExecutor;
        this.infra = infra;
        this.clock = clock;
        this.eventLog = eventLog;
        this.maxObservationAgeMillis = maxObservationAgeMillis;
    }

    private Object lockOf(String workloadId) {
        return locks.computeIfAbsent(workloadId, k -> new Object());
    }

    public EventLog eventLog() {
        return eventLog;
    }

    /**
     * 注册工作负载并创建初始副本（进程内实例）。重复注册幂等返回 false，不覆盖既有状态。
     */
    public boolean registerWorkload(String workloadId, int initialReplicas) {
        if (workloadId == null || workloadId.isBlank() || initialReplicas < 1) {
            throw new IllegalArgumentException("workloadId required and initialReplicas >= 1");
        }
        boolean created = registry.register(workloadId);
        if (!created) {
            return false;
        }
        synchronized (lockOf(workloadId)) {
            WorkloadAggregate agg = registry.aggregate(workloadId).orElseThrow();
            agg.setDesiredReplicas(initialReplicas);
            for (int i = 1; i <= initialReplicas; i++) {
                Instance instance = infra.provision(workloadId, agg.nextInstanceIndex(), clock.nowMillis());
                agg.instances().put(instance.getId(), instance);
            }
            eventLog.log(clock.nowMillis(), "register workload=" + workloadId
                    + " initialReplicas=" + initialReplicas);
        }
        return true;
    }

    /** 校验并写入策略；版本回退拒绝；更新后立即把 desiredReplicas 钳制进新区间并落地。 */
    public PolicyUpdateResult updatePolicy(String workloadId, ScalePolicy policy) {
        long now = clock.nowMillis();
        WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
        if (agg == null) {
            return PolicyUpdateResult.reject(RejectionReason.WORKLOAD_NOT_REGISTERED,
                    "workload not registered: " + workloadId);
        }
        synchronized (lockOf(workloadId)) {
            PolicyValidator.Validation validation = PolicyValidator.validate(policy);
            if (!validation.valid()) {
                eventLog.log(now, "policy REJECT workload=" + workloadId + " reason=" + validation.reason()
                        + " detail=" + validation.detail());
                return PolicyUpdateResult.reject(validation.reason(), validation.detail());
            }
            ScalePolicy current = agg.getPolicy();
            if (current != null && policy.version() <= current.version()) {
                eventLog.log(now, "policy VERSION-CONFLICT workload=" + workloadId
                        + " incoming=v" + policy.version() + " current=v" + current.version());
                return PolicyUpdateResult.reject(RejectionReason.POLICY_VERSION_CONFLICT,
                        "incoming version " + policy.version() + " <= current version " + current.version());
            }
            agg.setPolicy(policy);
            int before = agg.getDesiredReplicas();
            int clamped = Math.max(policy.minReplicas(), Math.min(policy.maxReplicas(), before));
            agg.setDesiredReplicas(clamped);
            eventLog.log(now, "policy APPLIED workload=" + workloadId + " version=v" + policy.version()
                    + " desiredReplicas " + before + " -> " + clamped
                    + " min=" + policy.minReplicas() + " max=" + policy.maxReplicas());
            applyDesiredReplicas(agg, now);
            return PolicyUpdateResult.ok(policy);
        }
    }

    public ObservationAcceptResult observeLoad(LoadObservation obs) {
        long now = clock.nowMillis();
        if (!registry.isRegistered(obs == null ? null : obs.workloadId())) {
            return ObservationAcceptResult.reject(RejectionReason.WORKLOAD_NOT_REGISTERED,
                    obs == null ? -1 : obs.version(),
                    "workload not registered: " + (obs == null ? null : obs.workloadId()));
        }
        synchronized (lockOf(obs.workloadId())) {
            ObservationAcceptResult result = observationStore.applyLoad(obs, now, maxObservationAgeMillis);
            eventLog.log(now, "load " + (result.accepted() ? "ACCEPT" : "REJECT " + result.rejectionReason())
                    + " workload=" + obs.workloadId() + " v=" + obs.version()
                    + " ratio=" + obs.loadRatio());
            return result;
        }
    }

    /**
     * 接入健康观测。被接受的不健康观测会把实例置为 UNHEALTHY 并自动请求修复；
     * 健康观测使无活动修复的实例自然恢复 READY。去重/预算/冷却由 RemediationService 保证。
     */
    public ObservationAcceptResult observeHealth(HealthObservation obs) {
        long now = clock.nowMillis();
        if (!registry.isRegistered(obs == null ? null : obs.workloadId())) {
            return ObservationAcceptResult.reject(RejectionReason.WORKLOAD_NOT_REGISTERED,
                    obs == null ? -1 : obs.version(),
                    "workload not registered: " + (obs == null ? null : obs.workloadId()));
        }
        synchronized (lockOf(obs.workloadId())) {
            ObservationAcceptResult result = observationStore.applyHealth(obs, now);
            if (!result.accepted()) {
                eventLog.log(now, "health REJECT " + result.rejectionReason()
                        + " workload=" + obs.workloadId() + " instance=" + obs.instanceId()
                        + " v=" + obs.version());
                return result;
            }
            Instance instance = registry.aggregate(obs.workloadId()).orElseThrow()
                    .instances().get(obs.instanceId());
            if (instance == null) {
                return result;
            }
            boolean hasActiveTask = hasActiveTask(registry.aggregate(obs.workloadId()).orElseThrow(),
                    obs.instanceId());
            if (!obs.healthy() && instance.getStatus() == InstanceStatus.READY && !hasActiveTask) {
                instance.setStatus(InstanceStatus.UNHEALTHY);
                eventLog.log(now, "health UNHEALTHY workload=" + obs.workloadId()
                        + " instance=" + obs.instanceId() + " v=" + obs.version()
                        + " -> auto remediation");
                RemediationRequestOutcome rr = remediationService.requestRemediation(
                        obs.workloadId(), obs.instanceId(), now);
                eventLog.log(now, "auto-remediation " + rr.result() + " workload=" + obs.workloadId()
                        + " instance=" + obs.instanceId()
                        + (rr.rejectionReason() != null ? " reason=" + rr.rejectionReason() : "")
                        + " rationale=" + rr.rationale());
            } else if (obs.healthy() && instance.getStatus() == InstanceStatus.UNHEALTHY && !hasActiveTask) {
                instance.setStatus(InstanceStatus.READY);
                eventLog.log(now, "health RECOVERED workload=" + obs.workloadId()
                        + " instance=" + obs.instanceId() + " v=" + obs.version());
            }
            return result;
        }
    }

    private static boolean hasActiveTask(WorkloadAggregate agg, String instanceId) {
        RemediationTask t = agg.tasksByInstance().get(instanceId);
        return t != null && t.isActive();
    }

    /** 显式请求修复（与健康事件自动触发共享同一套幂等规则）。 */
    public RemediationRequestOutcome requestRemediation(String workloadId, String instanceId) {
        if (!registry.isRegistered(workloadId)) {
            return new RemediationRequestOutcome(workloadId, instanceId,
                    com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationRequestResult.REJECTED,
                    null, RejectionReason.WORKLOAD_NOT_REGISTERED,
                    "workload not registered: " + workloadId);
        }
        synchronized (lockOf(workloadId)) {
            return remediationService.requestRemediation(workloadId, instanceId, clock.nowMillis());
        }
    }

    /** 计算目标副本数并立即把实例集合收敛到目标。 */
    public ScaleDecision reconcileScale(String workloadId) {
        long now = clock.nowMillis();
        WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
        if (agg == null) {
            return ScaleDecision.rejected(workloadId, RejectionReason.WORKLOAD_NOT_REGISTERED,
                    "workload not registered: " + workloadId, Long.MIN_VALUE, Long.MIN_VALUE);
        }
        synchronized (lockOf(workloadId)) {
            ScaleDecision decision = autoscaler.evaluate(agg, now, maxObservationAgeMillis);
            int activeBefore = agg.activeInstanceCount();
            applyDesiredReplicas(agg, now);
            int activeAfter = agg.activeInstanceCount();
            eventLog.log(now, "scale " + decision.outcome() + " workload=" + workloadId
                    + " replicas(active) " + activeBefore + " -> " + activeAfter
                    + " desired=" + agg.getDesiredReplicas()
                    + " policyV=" + decision.policyVersion() + " obsV=" + decision.observationVersion()
                    + " rationale=" + decision.rationale()
                    + (decision.rejectionReason() != null ? " reason=" + decision.rejectionReason() : ""));
            return decision;
        }
    }

    /**
     * 让实例数收敛到 desiredReplicas：
     * 扩容直接创建；缩容优先回收无活动修复的 UNHEALTHY 实例，其次按创建顺序回收 READY 实例。
     * 活动修复中的实例不会被回收（避免与修复竞争）。
     */
    private void applyDesiredReplicas(WorkloadAggregate agg, long now) {
        ScalePolicy policy = agg.getPolicy();
        if (policy == null) {
            return;
        }
        int active = agg.activeInstanceCount();
        int desired = Math.max(policy.minReplicas(), Math.min(policy.maxReplicas(), agg.getDesiredReplicas()));
        agg.setDesiredReplicas(desired);

        while (active < desired) {
            Instance instance = infra.provision(agg.getWorkloadId(), agg.nextInstanceIndex(), now);
            agg.instances().put(instance.getId(), instance);
            eventLog.log(now, "instance PROVISION " + instance.getId()
                    + " workload=" + agg.getWorkloadId());
            active++;
        }
        while (active > desired) {
            Instance victim = pickScaleDownVictim(agg);
            if (victim == null) {
                eventLog.log(now, "scale-down BLOCKED workload=" + agg.getWorkloadId()
                        + " active=" + active + " desired=" + desired
                        + " all remaining instances are under active remediation");
                break;
            }
            infra.terminate(victim, now);
            agg.instances().remove(victim.getId());
            eventLog.log(now, "instance TERMINATE " + victim.getId()
                    + " workload=" + agg.getWorkloadId() + " statusWas=" + victim.getStatus());
            active--;
        }
    }

    private Instance pickScaleDownVictim(WorkloadAggregate agg) {
        Instance healthyCandidate = null;
        for (Instance i : new ArrayList<>(agg.instances().values())) {
            if (i.getStatus() == InstanceStatus.TERMINATED) {
                continue;
            }
            if (hasActiveTask(agg, i.getId())) {
                continue;
            }
            if (i.getStatus() == InstanceStatus.UNHEALTHY) {
                return i; // 优先回收不健康实例
            }
            if (healthyCandidate == null) {
                healthyCandidate = i;
            }
        }
        return healthyCandidate;
    }

    /** 推进所有工作负载的修复任务（PENDING→RUNNING→终态），返回本轮终态迁移描述。 */
    public List<String> tickRemediations() {
        long now = clock.nowMillis();
        List<String> transitions = new ArrayList<>();
        for (WorkloadAggregate agg : registry.aggregates()) {
            synchronized (lockOf(agg.getWorkloadId())) {
                List<RemediationTask> finished = remediationExecutor.tick(agg, now);
                for (RemediationTask t : finished) {
                    transitions.add(agg.getWorkloadId() + ":" + t.getInstanceId() + "=" + t.getStatus());
                }
            }
        }
        return transitions;
    }

    /** 不可变状态快照，用于断言与解释。 */
    public WorkloadState snapshot(String workloadId) {
        WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
        if (agg == null) {
            return null;
        }
        synchronized (lockOf(workloadId)) {
            List<Instance> instances = new ArrayList<>(agg.instances().values());
            List<RemediationTask> tasks = new ArrayList<>(agg.taskHistory());
            return new WorkloadState(workloadId, agg.getPolicy(), agg.getLatestLoadVersion(),
                    agg.getLatestLoad(), agg.getDesiredReplicas(), agg.getLastScaleAtMillis(),
                    agg.getUpStreak(), agg.getDownStreak(), instances, tasks);
        }
    }

    public Map<String, Object> locksViewForTests() {
        return Map.of("locks", locks.size());
    }
}
