package com.hookshield.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Observability service providing centralized Micrometer metrics for real-time monitoring
 * via Prometheus and Spring Boot Actuator.
 */
@Service
public class MetricsService {

    private final MeterRegistry meterRegistry;
    private final Counter deduplicatedCounter;
    private final Timer dispatchLatencyTimer;

    public MetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.deduplicatedCounter = Counter.builder("webhooks.deduplicated.count")
            .description("Total number of duplicate webhooks dropped by idempotency engine")
            .register(meterRegistry);
        this.dispatchLatencyTimer = Timer.builder("webhooks.dispatch.latency")
            .description("Outbound HTTP dispatch roundtrip latency to subscriber endpoints")
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .register(meterRegistry);
    }

    public void incrementIngested(String tenantId, String provider) {
        Counter.builder("webhooks.ingested.total")
            .tag("tenant", tenantId != null ? tenantId : "unknown")
            .tag("provider", provider != null ? provider : "unknown")
            .description("Total incoming webhooks accepted at ingestion tier")
            .register(meterRegistry)
            .increment();
    }

    public void incrementDeduplicated() {
        deduplicatedCounter.increment();
    }

    public void incrementDlqRouted(String category) {
        Counter.builder("webhooks.dlq.routed.count")
            .tag("category", category != null ? category : "UNCLASSIFIED")
            .description("Total messages permanently routed to Dead-Letter Queue")
            .register(meterRegistry)
            .increment();
    }

    public void recordDispatchLatency(long durationMillis) {
        dispatchLatencyTimer.record(durationMillis, TimeUnit.MILLISECONDS);
    }

    public Timer.Sample startDispatchTimer() {
        return Timer.start(meterRegistry);
    }

    public void stopDispatchTimer(Timer.Sample sample) {
        if (sample != null) {
            sample.stop(dispatchLatencyTimer);
        }
    }
}
