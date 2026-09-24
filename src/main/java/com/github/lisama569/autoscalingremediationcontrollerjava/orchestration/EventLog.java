package com.github.lisama569.autoscalingremediationcontrollerjava.orchestration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 进程内事件日志：记录关键输入、状态迁移与判定依据。
 * 内容仅依赖显式传入的逻辑时钟，因此相同输入序列产生完全一致的日志（可复现）。
 */
public final class EventLog {

    private final List<String> entries = Collections.synchronizedList(new ArrayList<>());

    public void log(long nowMillis, String message) {
        entries.add("t=" + nowMillis + " | " + message);
    }

    public List<String> entries() {
        synchronized (entries) {
            return new ArrayList<>(entries);
        }
    }

    public void clear() {
        entries.clear();
    }

    public void dump() {
        for (String e : entries()) {
            System.out.println(e);
        }
    }
}
