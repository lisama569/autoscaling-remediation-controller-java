package com.github.lisama569.autoscalingremediationcontrollerjava.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 进程内决策日志：追加时同时打印一行结构化日志，内容包含
 * 关键输入（工作负载/实例/副本数）、判定结论与拒绝原因、状态版本与时间戳。
 * 相同输入序列重复执行会产生相同的日志序列，便于复现与审计。
 */
@Component
public final class DecisionLog {

    private static final Logger log = LoggerFactory.getLogger(DecisionLog.class);

    private final List<DecisionResult> entries = new ArrayList<>();

    public synchronized void append(DecisionResult result) {
        entries.add(result);
        if (log.isInfoEnabled()) {
            log.info("decision | accepted={} kind={} workload={} instance={} taskId={} "
                            + "replicas={}->{} reason={} detail='{}' at={} stateVersion={}",
                    result.accepted(),
                    result.kind(),
                    result.workloadId(),
                    result.instanceId(),
                    result.taskId(),
                    result.fromReplicas(),
                    result.toReplicas(),
                    result.reason(),
                    result.detail(),
                    result.atMillis(),
                    result.stateVersion());
        }
    }

    public synchronized List<DecisionResult> entries() {
        return List.copyOf(entries);
    }

    public synchronized List<DecisionResult> entriesOf(String workloadId) {
        List<DecisionResult> out = new ArrayList<>();
        for (DecisionResult e : entries) {
            if (workloadId.equals(e.workloadId())) {
                out.add(e);
            }
        }
        return List.copyOf(out);
    }

    public synchronized void clear() {
        entries.clear();
    }
}
