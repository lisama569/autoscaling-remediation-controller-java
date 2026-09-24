package com.github.lisama569.autoscalingremediationcontrollerjava.clock;

/**
 * 由测试/模拟器显式推进的虚拟时钟。线程安全；时间只能单调向前。
 */
public final class VirtualClock implements Clock {

    private long currentMillis;

    public VirtualClock(long startMillis) {
        this.currentMillis = startMillis;
    }

    @Override
    public synchronized long nowMillis() {
        return currentMillis;
    }

    public synchronized void advanceMillis(long deltaMillis) {
        if (deltaMillis < 0) {
            throw new IllegalArgumentException("deltaMillis must be non-negative: " + deltaMillis);
        }
        currentMillis += deltaMillis;
    }

    public synchronized void setMillis(long millis) {
        if (millis < currentMillis) {
            throw new IllegalArgumentException(
                    "virtual clock cannot move backwards: " + millis + " < " + currentMillis);
        }
        currentMillis = millis;
    }
}
