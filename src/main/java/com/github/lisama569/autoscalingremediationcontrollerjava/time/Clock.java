package com.github.lisama569.autoscalingremediationcontrollerjava.time;

/** 可替换的时钟，保证编排过程在测试中可复现。 */
public interface Clock {
    long nowMillis();
}
