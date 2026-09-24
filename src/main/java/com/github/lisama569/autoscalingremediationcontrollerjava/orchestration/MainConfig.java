package com.github.lisama569.autoscalingremediationcontrollerjava.orchestration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MainConfig {

    @Bean
    public EventLog eventLog() {
        return new EventLog();
    }
}
