package com.github.lisama569.autoscalingremediationcontrollerjava.store;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 工作负载注册表。注册表本身线程安全（ConcurrentHashMap）；
 * 单个工作负载内部状态的串行化由 {@code Controller} 按 workloadId 加锁保证。
 */
@Component
public class WorkloadRegistry {

    private final ConcurrentHashMap<String, WorkloadAggregate> workloads = new ConcurrentHashMap<>();

    /**
     * 注册新工作负载。
     *
     * @return true 表示新注册；false 表示已存在（重复注册幂等，不覆盖既有状态）
     */
    public boolean register(String workloadId) {
        return workloads.putIfAbsent(workloadId, new WorkloadAggregate(workloadId)) == null;
    }

    public boolean isRegistered(String workloadId) {
        return workloads.containsKey(workloadId);
    }

    /** 移除工作负载聚合（注销）。返回是否曾存在。 */
    public boolean remove(String workloadId) {
        return workloads.remove(workloadId) != null;
    }

    public Optional<WorkloadAggregate> aggregate(String workloadId) {
        return Optional.ofNullable(workloads.get(workloadId));
    }

    public Collection<WorkloadAggregate> aggregates() {
        return workloads.values();
    }
}
