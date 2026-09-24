package com.github.lisama569.autoscalingremediationcontrollerjava.time;

/** 测试与确定性仿真使用的手动时钟。 */
public final class ManualClock implements Clock {
    private volatile long now;

    public ManualClock() { this(0L); }
    public ManualClock(long startMillis) { this.now = startMillis; }

    @Override
    public long nowMillis() { return now; }

    public void set(long millis) { this.now = millis; }
    public void advance(long deltaMillis) { this.now += deltaMillis; }
}
