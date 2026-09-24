package com.github.lisama569.autoscalingremediationcontrollerjava.clock;

/**
 * 可替换的时钟抽象，使编排逻辑不依赖系统墙钟，便于确定性测试。
 */
public interface Clock {
    long nowMillis();
}
