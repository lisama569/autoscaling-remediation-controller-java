package com.github.lisama569.autoscalingremediationcontrollerjava.core;

import com.github.lisama569.autoscalingremediationcontrollerjava.domain.ScalingPolicy;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 策略合法性校验：任何非法策略都必须被拒绝并附带全部非法项，绝不静默修正或猜默认值。
 */
@Component
public final class PolicyValidator {

    public List<String> validate(ScalingPolicy policy) {
        if (policy == null) {
            return List.of("policy must not be null");
        }
        return policy.validate();
    }
}
