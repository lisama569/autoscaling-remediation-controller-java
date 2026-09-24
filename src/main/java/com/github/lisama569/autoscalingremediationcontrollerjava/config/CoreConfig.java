package com.github.lisama569.autoscalingremediationcontrollerjava.config;

import com.github.lisama569.autoscalingremediationcontrollerjava.clock.Clock;
import com.github.lisama569.autoscalingremediationcontrollerjava.clock.VirtualClock;
import com.github.lisama569.autoscalingremediationcontrollerjava.core.DecisionLog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 核心 Bean 装配。默认使用从 0 开始的虚拟时钟，保证本地运行与测试完全确定。
 */
@Configuration
public class CoreConfig {

    @Bean
    public Clock clock() {
        return new VirtualClock(0L);
    }

    @Bean
    public DecisionLog decisionLog() {
        return new DecisionLog();
    }
}
