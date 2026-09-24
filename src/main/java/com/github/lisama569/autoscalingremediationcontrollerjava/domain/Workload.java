package com.github.lisama569.autoscalingremediationcontrollerjava.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 工作负载聚合：策略、实例集合、分来源观测、伸缩/修复冷却时间点与单调状态版本。
 *
 * <p>并发规则：聚合内部所有可变状态都由 {@link #monitor()} 这把锁保护，
 * 任意编排操作以“每工作负载串行”的方式提交状态迁移，因此同一工作负载在任意
 * 线程交错下都不会出现重复/超额操作；跨工作负载操作互不阻塞。</p>
 *
 * <p>过期观测规则：观测按 sourceId 分别比较单调 sequence，序号回退/相等都不得覆盖。</p>
 */
public final class Workload {

    private final String id;
    private final Object lock = new Object();
    private ScalingPolicy policy;
    private final Map<String, Instance> instances = new LinkedHashMap<>();
    private final Map<String, LoadObservation> observationsBySource = new LinkedHashMap<>();
    private int currentReplicas;
    private long lastScaleUpAt = Long.MIN_VALUE / 2;
    private long lastScaleDownAt = Long.MIN_VALUE / 2;
    private long stateVersion;
    private int instanceSeq;
    private final Map<String, Integer> unhealthyStreak = new LinkedHashMap<>();
    private final Map<String, Long> lastRemediationAtByInstance = new LinkedHashMap<>();

    public Workload(String id, ScalingPolicy policy, int initialReplicas) {
        this.id = id;
        this.policy = policy;
        this.currentReplicas = initialReplicas;
    }

    public String getId() { return id; }
    public Object monitor() { return lock; }
    public long getStateVersion() { synchronized (lock) { return stateVersion; } }
    public ScalingPolicy getPolicy() { synchronized (lock) { return policy; } }
    public int getCurrentReplicas() { synchronized (lock) { return currentReplicas; } }
    public long getLastScaleUpAt() { synchronized (lock) { return lastScaleUpAt; } }
    public long getLastScaleDownAt() { synchronized (lock) { return lastScaleDownAt; } }

    /** 返回实例 id -> 状态 的不可变快照（视图在锁内构建）。 */
    public Map<String, InstanceStatus> instanceStatusSnapshot() {
        synchronized (lock) {
            Map<String, InstanceStatus> copy = new LinkedHashMap<>();
            instances.forEach((k, v) -> copy.put(k, v.getStatus()));
            return Map.copyOf(copy);
        }
    }

    public LoadObservation getLatestObservation() {
        synchronized (lock) {
            return latestOfAnySource();
        }
    }

    private LoadObservation latestOfAnySource() {
        // 确定性地选取：observedAt 最大；相同时间按 sourceId 字典序最大。
        LoadObservation best = null;
        for (LoadObservation o : observationsBySource.values()) {
            if (best == null
                    || o.observedAt() > best.observedAt()
                    || (o.observedAt() == best.observedAt()
                        && o.sourceId().compareTo(best.sourceId()) > 0)) {
                best = o;
            }
        }
        return best;
    }

    /**
     * @return 1 接受；0 重复（同来源相同序号）；-1 过期（序号回退）。
     */
    public int acceptObservation(LoadObservation obs) {
        synchronized (lock) {
            LoadObservation prev = observationsBySource.get(obs.sourceId());
            if (prev != null) {
                if (obs.sequence() == prev.sequence()) {
                    return 0;
                }
                if (obs.sequence() < prev.sequence()) {
                    return -1;
                }
            }
            observationsBySource.put(obs.sourceId(), obs);
            stateVersion++;
            return 1;
        }
    }

    public void updatePolicy(ScalingPolicy newPolicy) {
        synchronized (lock) {
            this.policy = newPolicy;
            this.stateVersion++;
        }
    }

    /** 创建并登记一个新实例，返回其实例 id。 */
    public String addInstance(InstanceStatus status) {
        synchronized (lock) {
            String iid = id + "-inst-" + (++instanceSeq);
            instances.put(iid, new Instance(iid, status));
            unhealthyStreak.put(iid, status == InstanceStatus.UNHEALTHY ? 1 : 0);
            stateVersion++;
            return iid;
        }
    }

    public Instance getInstance(String instanceId) {
        synchronized (lock) {
            return instances.get(instanceId);
        }
    }

    public void changeStatus(String instanceId, InstanceStatus status) {
        synchronized (lock) {
            Instance in = instances.get(instanceId);
            if (in != null) {
                in.setStatus(status);
                stateVersion++;
            }
        }
    }

    /**
     * 上报实例健康观测，返回新的连续不健康计数（达到策略门槛即视为应修复）。
     * 抖动的单帧不健康会被门槛吸收，不触发修复。
     */
    public int reportHealth(String instanceId, boolean healthy, long now) {
        synchronized (lock) {
            Instance in = instances.get(instanceId);
            if (in == null) {
                return -1;
            }
            if (healthy) {
                unhealthyStreak.merge(instanceId, 0, (a, b) -> 0);
                if (in.getStatus() != InstanceStatus.REPAIRING
                        && in.getStatus() != InstanceStatus.REPLACED
                        && in.getStatus() != InstanceStatus.TERMINATED) {
                    in.setStatus(InstanceStatus.HEALTHY);
                }
            } else {
                int streak = unhealthyStreak.getOrDefault(instanceId, 0) + 1;
                unhealthyStreak.put(instanceId, streak);
                if (in.getStatus() != InstanceStatus.REPAIRING) {
                    in.setStatus(InstanceStatus.UNHEALTHY);
                }
            }
            stateVersion++;
            return unhealthyStreak.getOrDefault(instanceId, 0);
        }
    }

    public int unhealthyStreakOf(String instanceId) {
        synchronized (lock) {
            return unhealthyStreak.getOrDefault(instanceId, 0);
        }
    }

    public long lastRemediationAtOf(String instanceId) {
        synchronized (lock) {
            return lastRemediationAtByInstance.getOrDefault(instanceId, Long.MIN_VALUE / 2);
        }
    }

    public void recordRemediation(String instanceId, long now) {
        synchronized (lock) {
            lastRemediationAtByInstance.put(instanceId, now);
        }
    }

    public void recordScale(long now, boolean scaleUp, int newReplicas) {
        synchronized (lock) {
            this.currentReplicas = newReplicas;
            if (scaleUp) {
                this.lastScaleUpAt = now;
            } else {
                this.lastScaleDownAt = now;
            }
            this.stateVersion++;
        }
    }

    /**
     * 达到不健康门槛、当前确实 UNHEALTHY、且不在修复中的实例。
     */
    public List<String> eligibleUnhealthyInstances() {
        synchronized (lock) {
            List<String> out = new ArrayList<>();
            int threshold = policy.unhealthyThreshold();
            for (Instance in : instances.values()) {
                if (in.getStatus() == InstanceStatus.UNHEALTHY
                        && unhealthyStreak.getOrDefault(in.getId(), 0) >= threshold) {
                    out.add(in.getId());
                }
            }
            return out;
        }
    }

    /** 确定性选择缩容目标：优先移除插入顺序最后的健康实例；绝不选中修复中的实例。 */
    public String chooseScaleDownTarget() {
        synchronized (lock) {
            String target = null;
            for (Instance in : instances.values()) {
                if (in.getStatus() == InstanceStatus.HEALTHY) {
                    target = in.getId();
                }
            }
            return target;
        }
    }

    /** 仍承载副本的实例数（修复中仍占位；已替换/终止/修复终态失败不占位）。 */
    public int activeReplicaCount() {
        synchronized (lock) {
            int n = 0;
            for (Instance in : instances.values()) {
                InstanceStatus s = in.getStatus();
                if (s != InstanceStatus.REPLACED
                        && s != InstanceStatus.TERMINATED
                        && s != InstanceStatus.REMEDIATION_FAILED) {
                    n++;
                }
            }
            return n;
        }
    }

    public int totalInstanceCount() {
        synchronized (lock) {
            return instances.size();
        }
    }
}
