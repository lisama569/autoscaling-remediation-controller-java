package com.github.lisama569.autoscalingremediationcontrollerjava.orchestration;

import com.github.lisama569.autoscalingremediationcontrollerjava.autoscale.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.autoscale.PolicyValidator;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityReservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacitySnapshot;
import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.SharedCapacityPool;
import com.github.lisama569.autoscalingremediationcontrollerjava.infra.SimulatedInfrastructure;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ObservationAcceptResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.PolicyUpdateResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RegistrationResult;
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
    private final SharedCapacityPool capacityPool;

    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    @org.springframework.beans.factory.annotation.Autowired
    public Controller(WorkloadRegistry registry,
                      ObservationStore observationStore,
                      Autoscaler autoscaler,
                      RemediationService remediationService,
                      RemediationExecutor remediationExecutor,
                      SimulatedInfrastructure infra,
                      Clock clock,
                      EventLog eventLog,
                      @Value("${arc.max-observation-age-millis:30000}") long maxObservationAgeMillis,
                      SharedCapacityPool capacityPool) {
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

    private Object lockOf(String workloadId) {
        return locks.computeIfAbsent(workloadId, k -> new Object());
    }

    public EventLog eventLog() {
        return eventLog;
    }

    /**
     * 注册工作负载并创建初始副本（进程内实例）。初始实例同样占用全局共享容量；
     * 容量不足时返回 {@link RejectionReason#SHARED_CAPACITY_EXHAUSTED}，不残留任何状态。
     * 重复注册幂等返回 {@code registered=false}，不覆盖既有状态。
     */
    public RegistrationResult registerWorkload(String workloadId, int initialReplicas) {
        if (workloadId == null || workloadId.isBlank() || initialReplicas < 1) {
            throw new IllegalArgumentException("workloadId required and initialReplicas >= 1");
        }
        boolean created = registry.register(workloadId);
        if (!created) {
            int existing = registry.aggregate(workloadId).orElseThrow().activeInstanceCount();
            return RegistrationResult.alreadyExists(workloadId, existing);
        }
        synchronized (lockOf(workloadId)) {
            long now = clock.nowMillis();
            WorkloadAggregate agg = registry.aggregate(workloadId).orElseThrow();
            // 初始实例同样从全局总盘占额度；不足则不创建任何实例并整体回滚注册
            CapacityReservation reservation = capacityPool.reserveInitial(workloadId, initialReplicas);
            if (!reservation.granted()) {
                registry.unregister(workloadId);
                locks.remove(workloadId);
                String rationale = "cannot register: requested " + initialReplicas
                        + " initial replicas but shared capacity only has "
                        + (reservation.totalCapacity() - reservation.usedCapacity())
                        + " free of " + reservation.totalCapacity();
                eventLog.log(now, "register REJECT workload=" + workloadId
                        + " reason=SHARED_CAPACITY_EXHAUSTED " + rationale);
                return RegistrationResult.rejected(workloadId,
                        RejectionReason.SHARED_CAPACITY_EXHAUSTED, rationale);
            }
            agg.setDesiredReplicas(initialReplicas);
            for (int i = 1; i <= initialReplicas; i++) {
                Instance instance = infra.provision(workloadId, agg.nextInstanceIndex(), now);
                agg.instances().put(instance.getId(), instance);
            }
            eventLog.log(now, "register workload=" + workloadId
                    + " initialReplicas=" + initialReplicas
                    + " capacity(used/total)=" + reservation.usedCapacity() + "/"
                    + reservation.totalCapacity());
        }
        return RegistrationResult.ok(workloadId, initialReplicas);
    }

    /**
     * 工作负载整体退出：终止其全部活动实例、把占用额度退回总盘并移出等待队列，
     * 随后按 FIFO 给之前因额度不足等待的工作负载补分配。
     */
    public boolean deregisterWorkload(String workloadId) {
        long now = clock.nowMillis();
        WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
        if (agg == null) {
            return false;
        }
        synchronized (lockOf(workloadId)) {
            int freed = 0;
            for (Instance i : new ArrayList<>(agg.instances().values())) {
                if (i.getStatus() != InstanceStatus.TERMINATED) {
                    infra.terminate(i, now);
                    freed++;
                }
            }
            capacityPool.releaseAllAndRemove(workloadId);
            registry.unregister(workloadId);
            locks.remove(workloadId);
            eventLog.log(now, "deregister workload=" + workloadId + " freedCapacity=" + freed
                    + " capacity(used/total)=" + capacityPool.usedCapacity() + "/"
                    + capacityPool.totalCapacity());
        }
        reconcilePendingCapacity();
        return true;
    }

    /**
     * 额度释放后按 FIFO 规则给等待中的工作负载补分配，返回本轮新授予的（工作负载, 份数）列表。
     *
     * <p>规则：
     * <ul>
     *   <li>只服务队首；队首一次被授予 1 份后若仍有未满足需求，自动回到<b>队尾</b>轮转，
     *       因此单个等待者不会霸占全部空闲额度，后来者也不会被无限期饿死；</li>
     *   <li>队首仍有需求但总盘已无空闲：立即停止本轮，队首位置保留，等待下一次释放；</li>
     *   <li>队首已无待满足需求（意向撤销/被新策略钳制）：移出队列后继续服务下一个；</li>
     *   <li>直到无空闲额度或无等待者为止。</li>
     * </ul>
     * 授予的额度立即落地为实例，保证账实一致。该方法在所有工作负载锁之外逐把取锁执行，
     * 可被任何释放额度的操作在锁外安全调用。
     */
    public List<Map.Entry<String, Integer>> reconcilePendingCapacity() {
        long now = clock.nowMillis();
        List<Map.Entry<String, Integer>> grants = new ArrayList<>();
        // 硬上限：循环每轮要么真实授予 1 份（至多 totalCapacity 次），要么移除一个
        // 永不复活的等待者（至多 totalCapacity 个工作负载），要么终止循环，杜绝活锁。
        int budget = capacityPool.totalCapacity() * 2;
        while (budget-- > 0) {
            String head = capacityPool.headWaiter();
            if (head == null) {
                break;
            }
            WorkloadAggregate agg = registry.aggregate(head).orElse(null);
            if (agg == null) {
                // 注册表里已不存在（理论上退出时会同步移除）：防御性清理，不占额度
                capacityPool.releaseAllAndRemove(head);
                continue;
            }
            synchronized (lockOf(head)) {
                GrantOutcome outcome = grantOnePendingUnit(agg, now, grants);
                if (outcome == GrantOutcome.GRANTED) {
                    continue; // 仍有需求时池内已把它放回队尾；无需求时已离队
                }
                if (outcome == GrantOutcome.UNAVAILABLE) {
                    break;   // 队首仍在等但盘子空了：保留队首位置，等待下一次释放
                }
                // NO_DEMAND：残留队列位置但已无未满足需求，移出后继续服务下一个等待者
                capacityPool.cancelWait(head);
                agg.setPendingCapacity(false);
            }
        }
        if (!grants.isEmpty()) {
            eventLog.log(now, "capacity drain grants=" + grants);
        }
        return grants;
    }

    /** 单次补分配尝试的结论。 */
    private enum GrantOutcome {
        /** 成功授予并落地 1 份（仍有需求时调用方已在池内被放回队尾轮转）。 */
        GRANTED,
        /** 队首仍有未满足需求，但总盘当前无空闲：必须停止本轮并保留队首位置。 */
        UNAVAILABLE,
        /** 已无待满足额度（目标已达成/被新策略钳低/意向撤销）：应移出队列。 */
        NO_DEMAND
    }

    /**
     * 给等待中的队首工作负载尝试落地 1 份额度。
     *
     * <p>目标（ceiling）取策略 [min,max] 对 desiredReplicas 的钳制值：授予后把 desired
     * 直接记为 ceiling，从而<b>保留该工作负载尚未满足的全部剩余需求</b>（例如一次性欠多份时），
     * 由后续轮转逐份补足；只有额度真正不足时才返回 {@link GrantOutcome#UNAVAILABLE} 继续等待。
     */
    private GrantOutcome grantOnePendingUnit(WorkloadAggregate agg, long now,
                                             List<Map.Entry<String, Integer>> grants) {
        String wl = agg.getWorkloadId();
        ScalePolicy policy = agg.getPolicy();
        int granted = capacityPool.allocationOf(wl);
        int ceiling = (policy == null) ? agg.getDesiredReplicas()
                : Math.max(policy.minReplicas(), Math.min(policy.maxReplicas(), agg.getDesiredReplicas()));
        if (granted >= ceiling) {
            return GrantOutcome.NO_DEMAND;
        }
        boolean stillWantsMore = granted + 1 < ceiling;
        // 仍有剩余需求时授予后回队尾轮转；这是最后一份则直接离队
        CapacityReservation reservation = capacityPool.grantQueuedUnit(wl, stillWantsMore);
        if (!reservation.granted()) {
            // 队首身份丢失（被并发操作处理）或额度已被并发抢光：保留队列位置继续等待
            return GrantOutcome.UNAVAILABLE;
        }
        // 记录完整目标而非 granted+1：一次性欠下的剩余份额继续留在需求里，随后续轮转补足
        agg.setDesiredReplicas(ceiling);
        provisionOne(agg, now);
        agg.setLastScaleAtMillis(now);
        boolean stillWaiting = capacityPool.isWaiting(wl);
        agg.setPendingCapacity(stillWaiting);
        grants.add(Map.entry(wl, 1));
        String waitInfo = stillWaiting
                ? " stillWaiting queuePosition=" + (capacityPool.snapshot().waitingOrder().indexOf(wl) + 1)
                        + " remaining=" + (ceiling - capacityPool.allocationOf(wl))
                : " queueLeft";
        eventLog.log(now, "capacity GRANT workload=" + wl
                + " replicas -> " + agg.activeInstanceCount()
                + " capacity(used/total)=" + reservation.usedCapacity() + "/" + reservation.totalCapacity()
                + waitInfo);
        return GrantOutcome.GRANTED;
    }

    public CapacitySnapshot capacitySnapshot() {
        return capacityPool.snapshot();
    }

    /**
     * 校验并写入策略；版本回退拒绝；更新后立即把 desiredReplicas 钳制进新区间并落地。
     *
     * <p>若新策略（例如下调 {@code maxReplicas}）引发缩容而把额度放回总盘，本方法在
     * <b>同一次操作内</b>释放工作负载锁后触发 FIFO 补分配：等待者当场拿到刚释放的额度，
     * 不需要任何不相干的后续事件带出。
     */
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
            applyDesiredReplicas(agg, now);
            result = PolicyUpdateResult.ok(policy);
        }
        // 锁外 drain：下调上限等策略变化可能刚释放了额度，等待者必须在本次操作内当场收敛
        if (capacityPool.waitingCount() > 0) {
            reconcilePendingCapacity();
        }
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

    /** 计算目标副本数并立即把实例集合收敛到目标；缩容释放额度后触发 FIFO 补分配。 */
    public ScaleDecision reconcileScale(String workloadId) {
        long now = clock.nowMillis();
        WorkloadAggregate agg = registry.aggregate(workloadId).orElse(null);
        if (agg == null) {
            return ScaleDecision.rejected(workloadId, RejectionReason.WORKLOAD_NOT_REGISTERED,
                    "workload not registered: " + workloadId, Long.MIN_VALUE, Long.MIN_VALUE);
        }
        ScaleDecision decision;
        int activeAfter;
        synchronized (lockOf(workloadId)) {
            decision = autoscaler.evaluate(agg, now, maxObservationAgeMillis);
            int activeBefore = agg.activeInstanceCount();
            applyDesiredReplicas(agg, now);
            activeAfter = agg.activeInstanceCount();
            eventLog.log(now, "scale " + decision.outcome() + " workload=" + workloadId
                    + " replicas(active) " + activeBefore + " -> " + activeAfter
                    + " desired=" + agg.getDesiredReplicas()
                    + " policyV=" + decision.policyVersion() + " obsV=" + decision.observationVersion()
                    + " capacity(used/total)=" + capacityPool.usedCapacity() + "/"
                    + capacityPool.totalCapacity()
                    + " waiting=" + capacityPool.waitingCount()
                    + " rationale=" + decision.rationale()
                    + (decision.rejectionReason() != null ? " reason=" + decision.rejectionReason() : ""));
        }
        // 锁外触发补分配：本次若是缩容（或策略变化）释放了额度，让等待者尽快收敛
        if (capacityPool.waitingCount() > 0) {
            reconcilePendingCapacity();
        }
        return decision;
    }

    /**
     * 让实例数收敛到 desiredReplicas。
     *
     * <p><b>额度语义：</b>扩容所需额度要么已由 Autoscaler（reconcile）预留，要么在此按
     * {@link SharedCapacityPool#reserveOrQueue} 申请（策略抬高 minReplicas 等路径）；
     * 额度已授予才创建实例，因此「Σ 活动实例数 ≤ 全局上限」恒成立。
     * 缩容每回收一个实例就归还 1 份额度。
     *
     * <p>缩容优先回收无活动修复的 UNHEALTHY 实例，其次按创建顺序回收 READY 实例；
     * 活动修复中的实例不会被回收（避免与修复竞争），此时缩容记录 scale-down BLOCKED。
     */
    private void applyDesiredReplicas(WorkloadAggregate agg, long now) {
        ScalePolicy policy = agg.getPolicy();
        if (policy == null) {
            return;
        }
        int active = agg.activeInstanceCount();
        int desired = Math.max(policy.minReplicas(), Math.min(policy.maxReplicas(), agg.getDesiredReplicas()));
        agg.setDesiredReplicas(desired);

        int granted = capacityPool.allocationOf(agg.getWorkloadId());
        while (active < desired) {
            if (active >= granted) {
                // 还没有为这一副本预留额度（策略抬底等非观测路径）：申请，不足则排队等待补分配
                CapacityReservation r = capacityPool.reserveOrQueue(agg.getWorkloadId(), 1);
                if (!r.granted()) {
                    agg.setPendingCapacity(true);
                    eventLog.log(now, "scale-up PENDING-CAPACITY workload=" + agg.getWorkloadId()
                            + " active=" + active + " desired=" + desired
                            + " queuePosition=" + r.queuePosition()
                            + " capacity(used/total)=" + r.usedCapacity() + "/" + r.totalCapacity());
                    break;
                }
                granted++;
            }
            provisionOne(agg, now);
            active++;
        }
        while (active > desired) {
            Instance victim = pickScaleDownVictim(agg);
            if (victim == null) {
                // 剩余实例都在活动修复中，无法回收：额度也不能提前归还（实例仍真实占用），
                // 等修复结束后的下一次收敛真正终止时再释放。
                eventLog.log(now, "scale-down BLOCKED workload=" + agg.getWorkloadId()
                        + " active=" + active + " desired=" + desired
                        + " all remaining instances are under active remediation;"
                        + " capacity stays reserved until termination");
                break;
            }
            infra.terminate(victim, now);
            agg.instances().remove(victim.getId());
            // 仅在实例真正终止的这一刻归还 1 份硬额度
            capacityPool.release(agg.getWorkloadId(), 1);
            eventLog.log(now, "instance TERMINATE " + victim.getId()
                    + " workload=" + agg.getWorkloadId() + " statusWas=" + victim.getStatus()
                    + " capacity(used/total)=" + capacityPool.usedCapacity() + "/"
                    + capacityPool.totalCapacity());
            active--;
        }
    }

    /** 创建并登记一个实例（调用方必须已为该副本预留额度）。 */
    private void provisionOne(WorkloadAggregate agg, long now) {
        Instance instance = infra.provision(agg.getWorkloadId(), agg.nextInstanceIndex(), now);
        agg.instances().put(instance.getId(), instance);
        eventLog.log(now, "instance PROVISION " + instance.getId()
                + " workload=" + agg.getWorkloadId());
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
        // 按 workloadId 排序后推进，保证多工作负载下处理顺序确定、可复现
        List<WorkloadAggregate> ordered = new ArrayList<>(registry.aggregates());
        ordered.sort(java.util.Comparator.comparing(WorkloadAggregate::getWorkloadId));
        for (WorkloadAggregate agg : ordered) {
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
