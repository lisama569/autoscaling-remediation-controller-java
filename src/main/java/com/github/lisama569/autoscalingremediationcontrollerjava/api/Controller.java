package com.github.lisama569.autoscalingremediationcontrollerjava.api;

import com.github.lisama569.autoscalingremediationcontrollerjava.api.dto.Dtos;
import com.github.lisama569.autoscalingremediationcontrollerjava.clock.VirtualClock;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.Autoscaler;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionLog;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionResult;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.RejectReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.RemediationManager;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.SimulationEngine;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.WorkloadRegistry;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.LoadObservation;
import com.github.lisama569.autoscalingremediationcontrollerjava.domain.Workload;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地编排 REST 接口（纯进程内状态，无外部服务依赖）。
 * 所有响应都包含 accepted/reason 等可区分结论；被拒绝的操作 HTTP 状态为 422。
 */
@RestController
@RequestMapping("/api")
public final class Controller {

    private final WorkloadRegistry registry;
    private final Autoscaler autoscaler;
    private final RemediationManager remediationManager;
    private final SimulationEngine simulationEngine;
    private final VirtualClock clock;
    private final DecisionLog decisionLog;

    public Controller(WorkloadRegistry registry, Autoscaler autoscaler,
                      RemediationManager remediationManager, SimulationEngine simulationEngine,
                      VirtualClock clock, DecisionLog decisionLog) {
        this.registry = registry;
        this.autoscaler = autoscaler;
        this.remediationManager = remediationManager;
        this.simulationEngine = simulationEngine;
        this.clock = clock;
        this.decisionLog = decisionLog;
    }

    @PostMapping("/workloads")
    public ResponseEntity<DecisionResult> register(@RequestBody Dtos.RegisterRequest request) {
        require(request != null, "request body required");
        require(request.workloadId() != null && !request.workloadId().isBlank(),
                "workloadId required");
        require(request.policy() != null, "policy required");
        int initial = request.initialReplicas() == null ? 0 : request.initialReplicas();
        return wrap(registry.register(request.workloadId(), request.policy(), initial));
    }

    @PostMapping("/workloads/{id}/policy")
    public ResponseEntity<DecisionResult> updatePolicy(@PathVariable String id,
                                       @RequestBody Dtos.UpdatePolicyRequest request) {
        require(request != null && request.policy() != null, "policy required");
        return wrap(registry.updatePolicy(id, request.policy()));
    }

    @PostMapping("/workloads/{id}/observations")
    public ResponseEntity<DecisionResult> observe(@PathVariable String id,
                                                  @RequestBody Dtos.ObserveRequest request) {
        require(request != null, "request body required");
        require(request.sourceId() != null && !request.sourceId().isBlank(), "sourceId required");
        require(request.sequence() != null, "sequence required");
        return wrap(registry.receiveObservation(id, request.sourceId(),
                request.sequence(), request.load()));
    }

    @PostMapping("/workloads/{id}/scale")
    public ResponseEntity<DecisionResult> scale(@PathVariable String id) {
        return wrap(autoscaler.evaluate(id));
    }

    @PostMapping("/workloads/{id}/health")
    public ResponseEntity<DecisionResult> health(@PathVariable String id,
                                                 @RequestBody Dtos.HealthRequest request) {
        require(request != null, "request body required");
        require(request.instanceId() != null && !request.instanceId().isBlank(),
                "instanceId required");
        require(request.healthy() != null, "healthy required");
        return wrap(registry.reportHealth(id, request.instanceId(), request.healthy()));
    }

    @PostMapping("/workloads/{id}/remediations/scan")
    public ResponseEntity<List<DecisionResult>> scan(@PathVariable String id) {
        return ResponseEntity.ok(remediationManager.scan(id));
    }

    @PostMapping("/workloads/{id}/remediations")
    public ResponseEntity<DecisionResult> remediate(@PathVariable String id,
                                    @RequestBody Dtos.RemediateRequest request) {
        require(request != null && request.instanceId() != null, "instanceId required");
        return wrap(remediationManager.trigger(id, request.instanceId(), "API"));
    }

    @PostMapping("/tick")
    public Map<String, Object> tick(@RequestBody(required = false) Dtos.TickRequest request) {
        long delta = request == null || request.deltaMillis() == null ? 0L : request.deltaMillis();
        List<DecisionResult> results = simulationEngine.tick(delta);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nowMillis", clock.nowMillis());
        out.put("decisions", results);
        return out;
    }

    @GetMapping("/workloads/{id}")
    public ResponseEntity<Map<String, Object>> view(@PathVariable String id) {
        Workload w = registry.get(id);
        if (w == null) {
            return ResponseEntity.notFound().build();
        }
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("workloadId", w.getId());
        view.put("stateVersion", w.getStateVersion());
        view.put("currentReplicas", w.getCurrentReplicas());
        view.put("activeReplicas", w.activeReplicaCount());
        view.put("policy", w.getPolicy());
        view.put("instances", w.instanceStatusSnapshot());
        LoadObservation obs = w.getLatestObservation();
        view.put("latestObservation", obs);
        view.put("activeRepairs", remediationManager.activeRepairCount(id));
        view.put("queuedRepairs", remediationManager.queuedRepairCount(id));
        view.put("remainingFailureBudget", remediationManager.remainingFailureBudget(id));
        return ResponseEntity.ok(view);
    }

    @GetMapping("/workloads")
    public List<String> list() {
        return registry.all().stream().map(Workload::getId).sorted().toList();
    }

    @GetMapping("/decisions")
    public List<DecisionResult> decisions() {
        return decisionLog.entries();
    }

    private static ResponseEntity<DecisionResult> wrap(DecisionResult result) {
        if (result.accepted() || result.reason() == RejectReason.NO_SCALE_NEEDED) {
            return ResponseEntity.ok(result);
        }
        return ResponseEntity.unprocessableEntity().body(result);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}
