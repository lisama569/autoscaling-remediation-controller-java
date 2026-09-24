package com.github.lisama569.autoscalingremediationcontrollerjava.time;

import org.springframework.stereotype.Component;

/** 墙钟：用于本地 HTTP 演示。确定性测试使用 ManualClock。 */
@Component
public class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
