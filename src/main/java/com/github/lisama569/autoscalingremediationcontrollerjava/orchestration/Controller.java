package com.github.lisama569.autoscalingremediationcontrollerjava.orchestration;

import com.github.lisama569.autoscalingremediationcontrollerjava.autoscale.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.autoscale.PolicyValidator;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityAcquireStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityPool;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityUnavailableException;
import com.github.lisama569.autoscalingremediationcontrollerjava.infra.SimulatedInfrastructure;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.CapacitySnapshot;
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
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleOutcome;
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
import java.util.Collections;
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
    private final CapacityPool capacityPool;

    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    public Controller(WorkloadRegistry registry,
                      ObservationStore observationStore,
                      Autoscaler autoscaler,
                      RemediationService remediationService,
                      RemediationExecutor remediationExecutor,
                      SimulatedInfrastructure infra,
                      Clock clock,
                      EventLog eventLog,
                      @Value("${arc.max-observation-age-millis:30000}") long maxObservationAgeMillis,
                      CapacityPool capacityPool) {
        this.registry = registry;
        this.observationStore = observationStore;
        this.autoscaler = autoscaler;
        this.remediationService = remediationService;
        this.remediationExecutor = remediationExecutor;
        this.infra = infra;
        this.clock = clock;
        this.eventLog = eventLog;
        this.maxObservationAgeMillis = maxObservationAgeMillis;
        this.capacityPool = capacityPool;
    }

    /** 全局共享容量账本（只读快照/查询用）。 */
    public CapacityPool capacityPool() {
        return capacityPool;
    }

    public CapacitySnapshot capacitySnapshot() {
        return capacityPool.snapshot();
    }

    private Object lockOf(String workloadId) {
        return locks.computeIfAbsent(workloadId, k -> new Object());
    }

    public EventLog eventLog() {
        return eventLog;
    }

    /**
     * 注册工作负载并创建初始副本（进程内实例）。重复注册幂等返回 false，不覆盖既有状态。
     *
     * <p>初始副本必须从全局容量总盘子一次性占足额度（FIFO：前方有等待者或额度不足时
     * 抛 {@link CapacityUnavailableException}，不留半成品）。加锁顺序：工作负载锁 → 容量锁。
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
            try {
                // 在工作负载锁内嵌套容量锁；失败则回滚注册，绝不留下占不到额度的工作负载
                capacityPool.reserveInitial(workloadId, initialReplicas);
            } catch (CapacityUnavailableException e) {
                registry.remove(workloadId);
                eventLog.log(clock.nowMillis(), "register REJECT workload=" + workloadId
                        + " initialReplicas=" + initialReplicas + " reason=INSUFFICIENT_GLOBAL_CAPACITY"
                        + " available=" + e.available() + " total=" + e.totalCapacity());
                throw e;
            }
            agg.setDesiredReplicas(initialReplicas);
            for (int i = 1; i <= initialReplicas; i++) {
                Instance instance = infra.provision(workloadId, agg.nextInstanceIndex(), clock.nowMillis());
                agg.instances().put(instance.getId(), instance);
            }
            eventLog.log(clock.nowMillis(), "register workload=" + workloadId
                    + " initialReplicas=" + initialReplicas
                    + " globalCapacity=" + capacityPool.snapshot().usedCapacity() + "/"
                    + capacityPool.snapshot().totalCapacity());
        }
        return true;
    }

    /**
     * 工作负载整体退出：释放其全部共享额度并移出等待队列与注册表。
     * 释放后立即（在锁外）尝试满足排队中的其他工作负载。
     */
    public boolean unregisterWorkload(String workloadId) {
        long now = clock.nowMillis();
        if (!registry.isRegistered(workloadId)) {
            return false;
        }
        synchronized (lockOf(workloadId)) {
            int freed = capacityPool.releaseAllAndRemove(workloadId);
            registry.remove(workloadId);
            eventLog.log(now, "unregister workload=" + workloadId + " freedCapacity=" + freed);
        }
        schedulePendingAllocations();
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
        PolicyUpdateResult result;
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
            applyDesiredReplicas(agg, now, 0);
            result = PolicyUpdateResult.ok(policy);
        }
        // 策略上调 min 可能产生新的扩容欠账，或此前的等待者因此可被满足：锁外统一调度
        schedulePendingAllocations();
        return result;
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

    /** 计算目标副本数并立即把实例集合收敛到目标；若发生缩容释放，随后在锁外驱动等待者补额。 */
    public ScaleDecision reconcileScale(String workloadId) {
        ScaleDecision decision = reconcileOnce(workloadId, clock.nowMillis());
        if (decision.outcome() == ScaleOutcome.SCALED_DOWN) {
            schedulePendingAllocations();
        }
        return decision;
    }

    /** 单次评估 + 收敛，不自我触发调度（供 {@link #schedulePendingAllocations} 在锁外复用）。 */
    private ScaleDecision reconcileOnce(String workloadId, long now) {
        return reconcileOnce(workloadId, now, null);
    }

    /**
     * @param provisionedSink 若非 null，累加本次实际新落地（占额并创建）的实例数；
     *                        容量调度据此精确判断本轮是否真有额度被分发（比只看结论更准，
     *                        因为策略上调 min 的补额可能发生在 NO_CHANGE 结论下）。
     */
    private ScaleDecision reconcileOnce(String workloadId, long now, int[] provisionedSink) {
        WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
        if (agg == null) {
            return ScaleDecision.rejected(workloadId, RejectionReason.WORKLOAD_NOT_REGISTERED,
                    "workload not registered: " + workloadId, Long.MIN_VALUE, Long.MIN_VALUE);
        }
        synchronized (lockOf(workloadId)) {
            // gateAcquired 记录本评估中闸门预占的额度份数（最多 1 份），apply 阶段直接落地，不得重复申请
            int[] gateAcquired = {0};
            ScaleDecision decision = autoscaler.evaluate(agg, now, maxObservationAgeMillis,
                    obsVersion -> {
                        boolean ok = capacityPool.tryAcquireFair(workloadId)
                                == CapacityAcquireStatus.ACQUIRED;
                        if (ok) {
                            gateAcquired[0]++;
                        }
                        return ok;
                    });
            int activeBefore = agg.activeInstanceCount();
            applyDesiredReplicas(agg, now, gateAcquired[0]);
            int activeAfter = agg.activeInstanceCount();
            if (provisionedSink != null && activeAfter > activeBefore) {
                provisionedSink[0] += activeAfter - activeBefore;
            }
            // 等待队列条目治理：
            //  - 已落地且无欠账（active==desired）：需求满足，移除自己的条目；
            //  - 仍有欠账：保留条目继续排队（apply 中再次申请会刷新队尾位置，实现轮流）；
            //  - CAPACITY_PENDING：条目由闸门内部登记，原地保留。
            if (decision.outcome() != ScaleOutcome.CAPACITY_PENDING
                    && agg.activeInstanceCount() >= agg.getDesiredReplicas()) {
                capacityPool.removeWaiter(workloadId);
            }
            eventLog.log(now, "scale " + decision.outcome() + " workload=" + workloadId
                    + " replicas(active) " + activeBefore + " -> " + activeAfter
                    + " desired=" + agg.getDesiredReplicas()
                    + " policyV=" + decision.policyVersion() + " obsV=" + decision.observationVersion()
                    + " globalUsed=" + capacityPool.snapshot().usedCapacity()
                    + "/" + capacityPool.snapshot().totalCapacity()
                    + " rationale=" + decision.rationale()
                    + (decision.rejectionReason() != null ? " reason=" + decision.rejectionReason() : ""));
            return decision;
        }
    }

    /**
     * 让实例数收敛到 desiredReplicas：
     * 扩容逐份向全局容量总盘子申请额度（FIFO 公平：额度不足或前方有等待者即停止本轮，
     * 工作负载留在等待队列，等额度释放后由 {@link #schedulePendingAllocations} 补上）；
     * 缩容优先回收无活动修复的 UNHEALTHY 实例，其次按创建顺序回收 READY 实例，每回收一个
     * 实例就向总盘退还一份额度。活动修复中的实例不会被回收（避免与修复竞争）。
     */
    /**
     * @param preAcquired 调用方已通过容量闸门占用、但实例尚未落地的额度份数（Autoscaler
     *                    扩容决策为 1）；这些份数在扩容循环里直接落地，不再重复申请。
     */
    private void applyDesiredReplicas(WorkloadAggregate agg, long now, int preAcquired) {
        String wl = agg.getWorkloadId();
        ScalePolicy policy = agg.getPolicy();
        if (policy == null) {
            return;
        }
        int active = agg.activeInstanceCount();
        int desired = Math.max(policy.minReplicas(), Math.min(policy.maxReplicas(), agg.getDesiredReplicas()));
        agg.setDesiredReplicas(desired);

        int pendingProvisions = preAcquired;
        while (active < desired) {
            if (pendingProvisions <= 0) {
                // 非评估来源（如策略上调 min）的欠账：每一份都走同一套公平申请
                CapacityAcquireStatus status = capacityPool.tryAcquireFair(wl);
                if (status != CapacityAcquireStatus.ACQUIRED) {
                    eventLog.log(now, "scale-up DEFERRED workload=" + wl
                            + " active=" + active + " desired=" + desired
                            + " reason=INSUFFICIENT_GLOBAL_CAPACITY queue=" + capacityPool.waitingQueue());
                    break;
                }
            } else {
                pendingProvisions--;
            }
            Instance instance = infra.provision(wl, agg.nextInstanceIndex(), now);
            agg.instances().put(instance.getId(), instance);
            eventLog.log(now, "instance PROVISION " + instance.getId()
                    + " workload=" + wl
                    + " globalUsed=" + capacityPool.snapshot().usedCapacity()
                    + "/" + capacityPool.snapshot().totalCapacity());
            active++;
        }
        while (active > desired) {
            Instance victim = pickScaleDownVictim(agg);
            if (victim == null) {
                eventLog.log(now, "scale-down BLOCKED workload=" + wl
                        + " active=" + active + " desired=" + desired
                        + " all remaining instances are under active remediation");
                break;
            }
            infra.terminate(victim, now);
            agg.instances().remove(victim.getId());
            capacityPool.releaseOne(wl); // 回收一份额度回总盘（账本与存活实例保持一致）
            eventLog.log(now, "instance TERMINATE " + victim.getId()
                    + " workload=" + wl + " statusWas=" + victim.getStatus()
                    + " globalUsed=" + capacityPool.snapshot().usedCapacity()
                    + "/" + capacityPool.snapshot().totalCapacity());
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
        // 按 workloadId 字典序遍历，避免依赖 ConcurrentHashMap 的非确定迭代顺序
        List<String> ids = new ArrayList<>();
        for (WorkloadAggregate a : registry.aggregates()) {
            ids.add(a.getWorkloadId());
        }
        Collections.sort(ids);
        for (String workloadId : ids) {
            WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
            if (agg == null) {
                continue;
            }
            synchronized (lockOf(workloadId)) {
                List<RemediationTask> finished = remediationExecutor.tick(agg, now);
                for (RemediationTask t : finished) {
                    transitions.add(workloadId + ":" + t.getInstanceId() + "=" + t.getStatus());
                }
            }
        }
        // tick 不改变实例数，但它是统一的“调度心跳”：随后驱动容量等待者（修复完成可能改变
        // 后续可缩容性等），保证多工作负载场景最终收敛。
        schedulePendingAllocations();
        return transitions;
    }

    /**
     * 容量补额调度（在所有工作负载锁之外执行，避免容量锁与工作负载锁嵌套成环）。
     *
     * <p>规则完全确定、与线程时序无关：
     * <ol>
     *   <li>每轮开始按 FIFO 抓取当前等待队列快照；</li>
     *   <li>逐个对队中工作负载执行一次 {@link #reconcileScale}：它内部的公平闸门只在该工作负载
     *       位于队首且有空闲额度时占用并落地；拿不到（额度不足/冷却/观测过期/重复观测…）就保持排队，
     *       跳过本轮，由后续事件或下一轮再评估；</li>
     *   <li>某轮只要发生过一次成功补额就再来一轮（拿到额度的工作负载重新排到队尾，形成轮流）；
     *       一整轮无人补额或队列清空则停止，因此必然终止且不会空转。</li>
     * </ol>
     * 返回本轮实际补额（SCALED_UP）次数。
     */
    public int schedulePendingAllocations() {
        int grants = 0;
        while (true) {
            List<String> queue = capacityPool.waitingQueue();
            if (queue.isEmpty()) {
                return grants;
            }
            int[] provisionedThisPass = {0};
            for (String workloadId : queue) {
                if (!registry.isRegistered(workloadId)) {
                    capacityPool.removeWaiter(workloadId);
                    continue;
                }
                reconcileOnce(workloadId, clock.nowMillis(), provisionedThisPass);
            }
            // 本轮没有任何额度被实际分发 -> 队中无人当前可补（冷却/过期/不需要/盘子仍空），停止，
            // 等后续缩容/退出/tick/新观测事件再次触发；有分发则再跑一轮以完成轮流与多份欠账。
            if (provisionedThisPass[0] == 0) {
                return grants;
            }
            grants += provisionedThisPass[0];
        }
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
