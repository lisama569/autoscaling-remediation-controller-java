package com.github.lisama569.autoscalingremediationcontrollerjava.web;

import com.github.lisama569.autoscalingremediationcontrollerjava.capacity.CapacityUnavailableException;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.CapacitySnapshot;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.HealthObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ObservationAcceptResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.PolicyUpdateResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.RemediationRequestOutcome;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScaleDecision;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.WorkloadState;
import com.github.lisama569.autoscalingremediationcontrollerjava.orchestration.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地演示接口：全部操作仅读写进程内状态，无外部依赖。
 * 默认使用 SystemClock（墙钟）；确定性验证请参考测试中的 ManualClock 用法。
 */
@RestController
@RequestMapping("/api/demo")
public class DemoController {

    private final Controller controller;

    public DemoController(Controller controller) {
        this.controller = controller;
    }

    @GetMapping("/ping")
    public Map<String, String> ping() {
        return Map.of("status", "ok");
    }

    @PostMapping("/workloads/{workloadId}")
    public Map<String, Object> register(@PathVariable String workloadId,
                                        @RequestParam(defaultValue = "1") int replicas) {
        boolean created = controller.registerWorkload(workloadId, replicas);
        return Map.of("registered", created, "workloadId", workloadId);
    }

    /** 注册时全局容量不足：明确返回 409 与可区分原因，不留半成品。 */
    @ExceptionHandler(CapacityUnavailableException.class)
    @ResponseStatus(org.springframework.http.HttpStatus.CONFLICT)
    public Map<String, Object> capacityUnavailable(CapacityUnavailableException e) {
        return Map.of(
                "registered", false,
                "reason", "INSUFFICIENT_GLOBAL_CAPACITY",
                "requested", e.requested(),
                "available", e.available(),
                "totalCapacity", e.totalCapacity(),
                "message", e.getMessage());
    }

    /** 工作负载整体退出：退还全部共享额度，随后尝试满足排队中的其他工作负载。 */
    @PostMapping("/workloads/{workloadId}/unregister")
    public Map<String, Object> unregister(@PathVariable String workloadId) {
        boolean removed = controller.unregisterWorkload(workloadId);
        return Map.of("unregistered", removed, "workloadId", workloadId,
                "capacity", controller.capacitySnapshot());
    }

    @PostMapping("/workloads/{workloadId}/policy")
    public PolicyUpdateResult policy(@PathVariable String workloadId, @RequestBody ScalePolicy policy) {
        return controller.updatePolicy(workloadId, policy);
    }

    @PostMapping("/workloads/{workloadId}/load")
    public ObservationAcceptResult load(@PathVariable String workloadId,
                                        @RequestParam long version,
                                        @RequestParam double ratio,
                                        @RequestParam(required = false) Long observedAt) {
        long ts = observedAt != null ? observedAt : System.currentTimeMillis();
        return controller.observeLoad(new LoadObservation(workloadId, version, ts, ratio));
    }

    @PostMapping("/workloads/{workloadId}/health/{instanceId}")
    public ObservationAcceptResult health(@PathVariable String workloadId,
                                          @PathVariable String instanceId,
                                          @RequestParam long version,
                                          @RequestParam boolean healthy,
                                          @RequestParam(required = false) Long observedAt) {
        long ts = observedAt != null ? observedAt : System.currentTimeMillis();
        return controller.observeHealth(new HealthObservation(workloadId, instanceId, version, ts, healthy));
    }

    @PostMapping("/workloads/{workloadId}/reconcile")
    public ScaleDecision reconcile(@PathVariable String workloadId) {
        return controller.reconcileScale(workloadId);
    }

    @PostMapping("/workloads/{workloadId}/remediate/{instanceId}")
    public RemediationRequestOutcome remediate(@PathVariable String workloadId,
                                               @PathVariable String instanceId) {
        return controller.requestRemediation(workloadId, instanceId);
    }

    @PostMapping("/tick")
    public Map<String, Object> tick() {
        List<String> transitions = controller.tickRemediations();
        return Map.of("transitions", transitions);
    }

    /** 显式驱动一次容量补额调度（FIFO），返回本轮实际补额（扩容）次数。 */
    @PostMapping("/capacity/schedule")
    public Map<String, Object> scheduleCapacity() {
        int grants = controller.schedulePendingAllocations();
        return Map.of("grantsThisRun", grants, "capacity", controller.capacitySnapshot());
    }

    /** 全局共享容量账本快照：总盘、已用、可用、各工作负载占用与 FIFO 等待队列。 */
    @GetMapping("/capacity")
    public CapacitySnapshot capacity() {
        return controller.capacitySnapshot();
    }

    @GetMapping("/workloads/{workloadId}")
    public WorkloadState snapshot(@PathVariable String workloadId) {
        return controller.snapshot(workloadId);
    }

    @GetMapping("/events")
    public Map<String, Object> events() {
        return new LinkedHashMap<>(Map.of("events", controller.eventLog().entries()));
    }
}
