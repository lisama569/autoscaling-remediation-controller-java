package com.github.lisama569.autoscalingremediationcontrollerjava.autoscale;

import com.github.lisama569.autoscalingremediationcontrollerjava.model.RejectionReason;
import com.github.lisama569.autoscalingremediationcontrollerjava.model.ScalePolicy;

/**
 * 策略合法性校验。任何非法字段都必须返回 {@link RejectionReason#INVALID_POLICY} 及具体原因，
 * 不允许以异常或模糊错误表达。
 */
public final class PolicyValidator {

    public record Validation(boolean valid, RejectionReason reason, String detail) {
        public static Validation ok() {
            return new Validation(true, null, "valid");
        }

        public static Validation invalid(String detail) {
            return new Validation(false, RejectionReason.INVALID_POLICY, detail);
        }
    }

    private PolicyValidator() {
    }

    public static Validation validate(ScalePolicy p) {
        if (p == null) {
            return Validation.invalid("policy is null");
        }
        if (p.version() < 1) {
            return Validation.invalid("version must be >= 1, got " + p.version());
        }
        if (p.minReplicas() < 1) {
            return Validation.invalid("minReplicas must be >= 1, got " + p.minReplicas());
        }
        if (p.maxReplicas() < p.minReplicas()) {
            return Validation.invalid("maxReplicas " + p.maxReplicas()
                    + " must be >= minReplicas " + p.minReplicas());
        }
        if (!(p.scaleDownThreshold() > 0.0d)) {
            return Validation.invalid("scaleDownThreshold must be > 0, got " + p.scaleDownThreshold());
        }
        if (!(p.scaleDownThreshold() <= p.scaleUpThreshold())) {
            return Validation.invalid("scaleDownThreshold " + p.scaleDownThreshold()
                    + " must be <= scaleUpThreshold " + p.scaleUpThreshold());
        }
        if (!(p.scaleUpThreshold() < 1.0d)) {
            return Validation.invalid("scaleUpThreshold must be < 1, got " + p.scaleUpThreshold());
        }
        if (p.stabilizationRounds() < 1) {
            return Validation.invalid("stabilizationRounds must be >= 1, got " + p.stabilizationRounds());
        }
        if (p.cooldownMillis() < 0) {
            return Validation.invalid("cooldownMillis must be >= 0, got " + p.cooldownMillis());
        }
        if (p.remediationConcurrency() < 1) {
            return Validation.invalid("remediationConcurrency must be >= 1, got "
                    + p.remediationConcurrency());
        }
        if (p.failureBudget() < 1) {
            return Validation.invalid("failureBudget must be >= 1, got " + p.failureBudget());
        }
        if (p.failureBudgetWindowMillis() <= 0) {
            return Validation.invalid("failureBudgetWindowMillis must be > 0, got "
                    + p.failureBudgetWindowMillis());
        }
        if (p.remediationCooldownMillis() < 0) {
            return Validation.invalid("remediationCooldownMillis must be >= 0, got "
                    + p.remediationCooldownMillis());
        }
        if (p.remediationTimeoutMillis() <= 0) {
            return Validation.invalid("remediationTimeoutMillis must be > 0, got "
                    + p.remediationTimeoutMillis());
        }
        return Validation.ok();
    }
}
