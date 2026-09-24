package com.github.lisama569.autoscalingremediationcontrollerjava.store;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ObservationAcceptResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import org.springframework.stereotype.Component;

/**
 * 观测接入。所有方法在调用方持有该工作负载串行锁的前提下执行。
 *
 * <p>规则：
 * <ul>
 *   <li>版本必须严格单调递增，回退/重复版本以 {@code POLICY_VERSION_CONFLICT} 拒绝（不覆盖状态）；</li>
 *   <li>观测时间早于已知观测、或超过 maxAge 视为过期，以 {@code STALE_LOAD_OBSERVATION} 拒绝；</li>
 *   <li>工作负载未注册以 {@code WORKLOAD_NOT_REGISTERED} 拒绝。</li>
 * </ul>
 */
@Component
public class ObservationStore {

    private final WorkloadRegistry registry;

    public ObservationStore(WorkloadRegistry registry) {
        this.registry = registry;
    }

    public ObservationAcceptResult applyLoad(LoadObservation obs, long nowMillis, long maxAgeMillis) {
        if (obs == null || obs.workloadId() == null) {
            return ObservationAcceptResult.reject(
                    RejectionReason.ILLEGAL_ARGUMENT, -1, "load observation is null");
        }
        WorkloadAggregate agg = registry.aggregate(obs.workloadId()).orElse(null);
        if (agg == null) {
            return ObservationAcceptResult.reject(
                    RejectionReason.WORKLOAD_NOT_REGISTERED, obs.version(),
                    "workload not registered: " + obs.workloadId());
        }
        if (obs.version() <= agg.getLatestLoadVersion()) {
            return ObservationAcceptResult.reject(
                    RejectionReason.POLICY_VERSION_CONFLICT, obs.version(),
                    "load version " + obs.version() + " <= accepted version " + agg.getLatestLoadVersion());
        }
        if (agg.getLatestLoad() != null && obs.observedAtMillis() < agg.getLatestLoad().observedAtMillis()) {
            return ObservationAcceptResult.reject(
                    RejectionReason.STALE_LOAD_OBSERVATION, obs.version(),
                    "observedAt " + obs.observedAtMillis() + " < latest "
                            + agg.getLatestLoad().observedAtMillis());
        }
        if (obs.observedAtMillis() > nowMillis) {
            return ObservationAcceptResult.reject(
                    RejectionReason.STALE_LOAD_OBSERVATION, obs.version(),
                    "observation from the future: " + obs.observedAtMillis() + " > now " + nowMillis);
        }
        if (nowMillis - obs.observedAtMillis() > maxAgeMillis) {
            return ObservationAcceptResult.reject(
                    RejectionReason.STALE_LOAD_OBSERVATION, obs.version(),
                    "load observation older than maxAge " + maxAgeMillis + "ms");
        }
        if (Double.isNaN(obs.loadRatio()) || obs.loadRatio() < 0.0d) {
            return ObservationAcceptResult.reject(
                    RejectionReason.ILLEGAL_ARGUMENT, obs.version(),
                    "loadRatio must be >= 0, got " + obs.loadRatio());
        }
        agg.setLatestLoad(obs);
        return ObservationAcceptResult.ok(obs.version(),
                "load v" + obs.version() + " ratio=" + obs.loadRatio() + " accepted");
    }

    public ObservationAcceptResult applyHealth(HealthObservation obs, long nowMillis) {
        if (obs == null || obs.workloadId() == null || obs.instanceId() == null) {
            return ObservationAcceptResult.reject(
                    RejectionReason.ILLEGAL_ARGUMENT, -1, "health observation is null");
        }
        WorkloadAggregate agg = registry.aggregate(obs.workloadId()).orElse(null);
        if (agg == null) {
            return ObservationAcceptResult.reject(
                    RejectionReason.WORKLOAD_NOT_REGISTERED, obs.version(),
                    "workload not registered: " + obs.workloadId());
        }
        long acceptedVersion = agg.getLatestHealthVersion(obs.instanceId());
        if (obs.version() <= acceptedVersion) {
            return ObservationAcceptResult.reject(
                    RejectionReason.POLICY_VERSION_CONFLICT, obs.version(),
                    "health version " + obs.version() + " <= accepted version " + acceptedVersion
                            + " for instance " + obs.instanceId());
        }
        var previous = agg.getLatestHealth(obs.instanceId());
        if (previous != null && obs.observedAtMillis() < previous.observedAtMillis()) {
            return ObservationAcceptResult.reject(
                    RejectionReason.STALE_LOAD_OBSERVATION, obs.version(),
                    "health observedAt " + obs.observedAtMillis() + " < latest "
                            + previous.observedAtMillis() + " for instance " + obs.instanceId());
        }
        if (obs.observedAtMillis() > nowMillis) {
            return ObservationAcceptResult.reject(
                    RejectionReason.STALE_LOAD_OBSERVATION, obs.version(),
                    "health observation from the future: " + obs.observedAtMillis());
        }
        agg.putLatestHealth(obs);
        return ObservationAcceptResult.ok(obs.version(),
                "health v" + obs.version() + " instance=" + obs.instanceId()
                        + " healthy=" + obs.healthy() + " accepted");
    }
}
