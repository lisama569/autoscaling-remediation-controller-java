package com.github.lisama569.autoscalingremediationcontrollerjava.infra;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.Instance;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.InstanceStatus;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 进程内“基础设施”：实例创建、终止、修复/替换全部在内存中完成，
 * 不访问任何外部服务。修复行为可按实例脚本化（失败次数、耗时、超时），
 * 因此相同输入序列必然产生相同结果（可复现）。
 */
@Component
public class SimulatedInfrastructure {

    /** 脚本化的单次修复结果；队列为空后回退到默认（成功 + 默认耗时）。 */
    public record ScriptedRepair(boolean success, boolean timedOut, long durationMillis) {
    }

    static final long DEFAULT_REPAIR_DURATION_MILLIS = 100L;

    private final Map<String, Instance> allInstances = new LinkedHashMap<>();
    private final Map<String, Deque<ScriptedRepair>> scripts = new LinkedHashMap<>();

    public Instance provision(String workloadId, int index, long nowMillis) {
        String id = workloadId + "-inst-" + index;
        Instance instance = new Instance(id, workloadId, InstanceStatus.READY, nowMillis);
        allInstances.put(id, instance);
        return instance;
    }

    /** 为某实例排队一条脚本化修复结果（可多次调用模拟抖动后恢复）。 */
    public void scriptRepair(String instanceId, boolean success, boolean timedOut, long durationMillis) {
        scripts.computeIfAbsent(instanceId, k -> new ArrayDeque<>())
                .addLast(new ScriptedRepair(success, timedOut, durationMillis));
    }

    /** 任务启动时取计划：消费一条脚本，否则默认成功。 */
    public RepairPlan plan(Instance instance) {
        Deque<ScriptedRepair> q = scripts.get(instance.getId());
        ScriptedRepair s = (q == null || q.isEmpty()) ? null : q.pollFirst();
        if (s == null) {
            return new RepairPlan(true, false, DEFAULT_REPAIR_DURATION_MILLIS);
        }
        return new RepairPlan(s.success(), s.timedOut(), s.durationMillis());
    }

    /**
     * 修复完成时应用副作用。
     *
     * @param mode REPAIR：原地恢复；REPLACE：旧实例终止并创建替换实例
     * @return 执行结果；替换时 replacedInstanceId 携带新实例 id
     */
    public RepairExecution completeRepair(Instance instance, RepairMode mode, boolean success, long nowMillis) {
        if (!success) {
            instance.setStatus(InstanceStatus.UNHEALTHY);
            return new RepairExecution(false, 0L, null, "repair failed for " + instance.getId());
        }
        if (mode == RepairMode.REPLACE) {
            instance.setStatus(InstanceStatus.TERMINATED);
            String newId = instance.getId() + "-repl";
            Instance replacement = new Instance(newId, instance.getWorkloadId(), InstanceStatus.READY, nowMillis);
            allInstances.put(newId, replacement);
            return new RepairExecution(true, 0L, newId, "replaced " + instance.getId() + " with " + newId);
        }
        instance.setStatus(InstanceStatus.READY);
        return new RepairExecution(true, 0L, null, "repaired " + instance.getId());
    }

    public void terminate(Instance instance, long nowMillis) {
        instance.setStatus(InstanceStatus.TERMINATED);
    }

    public List<Instance> snapshot(String workloadId) {
        List<Instance> out = new ArrayList<>();
        for (Instance i : allInstances.values()) {
            if (i.getWorkloadId().equals(workloadId)) {
                out.add(i);
            }
        }
        return out;
    }
}
