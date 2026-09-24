package com.github.lisama569.autoscalingremediationcontrollerjava.store;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationTask;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个工作负载的可变聚合根。
 *
 * <p><b>不变量（所有读写必须持有该工作负载的串行锁，见 {@link Controller}）：</b>
 * <ul>
 *   <li>策略版本、观测版本只允许单调递增；</li>
 *   <li>desiredReplicas 始终位于 [policy.minReplicas, policy.maxReplicas]；</li>
 *   <li>同一实例同一时刻至多存在一个活动修复任务；</li>
 *   <li>failureTimes 仅保留当前预算窗口内的失败时间戳。</li>
 * </ul>
 */
public final class WorkloadAggregate {

    private final String workloadId;

    private ScalePolicy policy;

    private LoadObservation latestLoad;
    private long latestLoadVersion = Long.MIN_VALUE;

    /** instanceId -> 最新健康观测；版本号单独记录用于乱序拒绝。 */
    private final Map<String, HealthObservation> latestHealth = new LinkedHashMap<>();
    private final Map<String, Long> latestHealthVersion = new LinkedHashMap<>();

    private int desiredReplicas;
    private long lastScaleAtMillis = Long.MIN_VALUE;
    private int upStreak;
    private int downStreak;

    /** 上一次真正引发伸缩动作的观测版本：同一条观测重复评估不得重复伸缩。 */
    private long lastActedObservationVersion = Long.MIN_VALUE;

    private final LinkedHashMap<String, Instance> instances = new LinkedHashMap<>();
    private int instanceSequence;

    /** instanceId -> 任务（含已终止任务，用于冷却与去重判定）。 */
    private final LinkedHashMap<String, RemediationTask> tasksByInstance = new LinkedHashMap<>();
    private final List<RemediationTask> taskHistory = new ArrayList<>();

    /** 当前预算窗口内的失败时间戳。 */
    private final Deque<Long> failureTimes = new ArrayDeque<>();

    public WorkloadAggregate(String workloadId) {
        this.workloadId = workloadId;
    }

    public String getWorkloadId() {
        return workloadId;
    }

    public ScalePolicy getPolicy() {
        return policy;
    }

    public void setPolicy(ScalePolicy policy) {
        this.policy = policy;
    }

    public LoadObservation getLatestLoad() {
        return latestLoad;
    }

    public long getLatestLoadVersion() {
        return latestLoadVersion;
    }

    public void setLatestLoad(LoadObservation obs) {
        this.latestLoad = obs;
        this.latestLoadVersion = obs.version();
    }

    public HealthObservation getLatestHealth(String instanceId) {
        return latestHealth.get(instanceId);
    }

    public long getLatestHealthVersion(String instanceId) {
        return latestHealthVersion.getOrDefault(instanceId, Long.MIN_VALUE);
    }

    public void putLatestHealth(HealthObservation obs) {
        latestHealth.put(obs.instanceId(), obs);
        latestHealthVersion.put(obs.instanceId(), obs.version());
    }

    public Map<String, HealthObservation> healthView() {
        return latestHealth;
    }

    public int getDesiredReplicas() {
        return desiredReplicas;
    }

    public void setDesiredReplicas(int desiredReplicas) {
        this.desiredReplicas = desiredReplicas;
    }

    public long getLastScaleAtMillis() {
        return lastScaleAtMillis;
    }

    public void setLastScaleAtMillis(long v) {
        this.lastScaleAtMillis = v;
    }

    public int getUpStreak() {
        return upStreak;
    }

    public void setUpStreak(int v) {
        this.upStreak = v;
    }

    public int getDownStreak() {
        return downStreak;
    }

    public void setDownStreak(int v) {
        this.downStreak = v;
    }

    public long getLastActedObservationVersion() {
        return lastActedObservationVersion;
    }

    public void setLastActedObservationVersion(long v) {
        this.lastActedObservationVersion = v;
    }

    public LinkedHashMap<String, Instance> instances() {
        return instances;
    }

    public int nextInstanceIndex() {
        return ++instanceSequence;
    }

    public LinkedHashMap<String, RemediationTask> tasksByInstance() {
        return tasksByInstance;
    }

    public List<RemediationTask> taskHistory() {
        return taskHistory;
    }

    public Deque<Long> failureTimes() {
        return failureTimes;
    }

    /** 活动（未终止）实例数。 */
    public int activeInstanceCount() {
        int n = 0;
        for (Instance i : instances.values()) {
            if (i.getStatus() != com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus.TERMINATED) {
                n++;
            }
        }
        return n;
    }
}
