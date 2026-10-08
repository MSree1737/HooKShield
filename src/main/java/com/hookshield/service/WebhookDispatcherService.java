package com.hookshield.service;

import com.hookshield.config.KafkaConfig;
import com.hookshield.domain.enums.DeliveryStatus;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.repository.WebhookDeliveryRepository;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dispatcher executing outbound HTTP calls to subscriber endpoints using Spring RestClient
 * on Java 21 Virtual Threads, integrated with Resilience4j circuit breakers and non-blocking retry pipelines.
 */
@Service
public class WebhookDispatcherService {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcherService.class);

    private final RestClient.Builder restClientBuilder;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final WebhookProducerService producerService;
    private final WebhookDeliveryRepository deliveryRepository;
    private final MetricsService metricsService;
    private final TransactionTemplate transactionTemplate;
    private final ConcurrentHashMap<String, RestClient> restClients = new ConcurrentHashMap<>();

    public WebhookDispatcherService(RestClient.Builder restClientBuilder,
                                    CircuitBreakerRegistry circuitBreakerRegistry,
                                    WebhookProducerService producerService,
                                    WebhookDeliveryRepository deliveryRepository,
                                    MetricsService metricsService,
                                    PlatformTransactionManager transactionManager) {
        this.restClientBuilder = restClientBuilder;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.producerService = producerService;
        this.deliveryRepository = deliveryRepository;
        this.metricsService = metricsService;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Dispatches the webhook payload to the subscriber endpoint with circuit breaker evaluation.
     */
    public void dispatch(WebhookIngestPayload payload) {
        String targetEndpoint = payload.targetEndpoint();
        String host = extractHost(targetEndpoint);
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(host);

        // Circuit Breaker Fast-Fail Divert: if OPEN, divert directly to retry topic without network call
        if (CircuitBreaker.State.OPEN.equals(circuitBreaker.getState()) ||
            CircuitBreaker.State.FORCED_OPEN.equals(circuitBreaker.getState())) {
            log.warn("Circuit breaker for host {} is OPEN. Diverting webhook {} directly to retry pipeline.",
                host, payload.deliveryId());
            handleDispatchFailure(payload, "Circuit Breaker OPEN for host: " + host, 503);
            return;
        }

        Timer.Sample timerSample = metricsService.startDispatchTimer();
        RestClient restClient = getOrCreateRestClient(targetEndpoint);

        try {
            circuitBreaker.executeRunnable(() -> executeOutboundCall(restClient, targetEndpoint, payload));
            metricsService.stopDispatchTimer(timerSample);

            // Successfully delivered
            markDeliverySuccess(payload);
        } catch (CallNotPermittedException e) {
            metricsService.stopDispatchTimer(timerSample);
            log.warn("Call not permitted by circuit breaker for host {}: {}", host, e.getMessage());
            handleDispatchFailure(payload, "Circuit breaker tripped open: " + e.getMessage(), 503);
        } catch (HttpServerErrorException e) {
            metricsService.stopDispatchTimer(timerSample);
            log.warn("Downstream subscriber {} returned server error {}: {}",
                targetEndpoint, e.getStatusCode(), e.getMessage());
            handleDispatchFailure(payload, e.getMessage(), e.getStatusCode().value());
        } catch (HttpClientErrorException e) {
            metricsService.stopDispatchTimer(timerSample);
            log.warn("Downstream subscriber {} returned client error {}: {}",
                targetEndpoint, e.getStatusCode(), e.getMessage());
            handleDispatchFailure(payload, e.getMessage(), e.getStatusCode().value());
        } catch (Exception e) {
            metricsService.stopDispatchTimer(timerSample);
            log.error("Downstream outbound dispatch failed for delivery {}: {}", payload.deliveryId(), e.getMessage());
            handleDispatchFailure(payload, e.getMessage(), 500);
        }
    }

    private void executeOutboundCall(RestClient restClient, String endpoint, WebhookIngestPayload payload) {
        restClient.post()
            .uri(endpoint)
            .contentType(MediaType.APPLICATION_JSON)
            .headers(httpHeaders -> {
                if (payload.headers() != null) {
                    for (Map.Entry<String, String> entry : payload.headers().entrySet()) {
                        // Avoid overriding host / content-length headers
                        if (!HttpHeaders.HOST.equalsIgnoreCase(entry.getKey()) &&
                            !HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(entry.getKey())) {
                            httpHeaders.add(entry.getKey(), entry.getValue());
                        }
                    }
                }
                httpHeaders.add("X-HookShield-Delivery-ID", payload.deliveryId().toString());
            })
            .body(payload.rawPayload() != null ? payload.rawPayload() : "{}")
            .retrieve()
            .toBodilessEntity();
    }

    public void markDeliverySuccess(WebhookIngestPayload payload) {
        transactionTemplate.executeWithoutResult(status ->
            deliveryRepository.updateStatus(payload.deliveryId(), DeliveryStatus.DISPATCHED)
        );
        log.info("Successfully delivered webhook {} to {}", payload.deliveryId(), payload.targetEndpoint());
    }

    /**
     * Non-blocking multi-tier retry routing:
     * Attempt 0 -> webhook-retry-1m (1 min delay)
     * Attempt 1 -> webhook-retry-5m (5 min delay)
     * Attempt 2 -> webhook-retry-15m (15 min delay)
     * Attempt >= 3 -> webhook-dead-letter-queue (DLQ)
     */
    public void handleDispatchFailure(WebhookIngestPayload payload, String errorMessage, int statusCode) {
        int currentAttempt = payload.retryCount();
        long now = Instant.now().toEpochMilli();

        if (currentAttempt == 0) {
            int nextAttempt = 1;
            transactionTemplate.executeWithoutResult(status ->
                deliveryRepository.updateStatusAndRetryCount(payload.deliveryId(), DeliveryStatus.RETRYING, nextAttempt)
            );
            long nextScheduled = now + 60_000L; // +1m
            producerService.sendRetry(KafkaConfig.TOPIC_RETRY_1M, payload.withRetryCount(nextAttempt), nextScheduled);
            log.info("Routed delivery {} to retry-1m (attempt 1)", payload.deliveryId());
        } else if (currentAttempt == 1) {
            int nextAttempt = 2;
            transactionTemplate.executeWithoutResult(status ->
                deliveryRepository.updateStatusAndRetryCount(payload.deliveryId(), DeliveryStatus.RETRYING, nextAttempt)
            );
            long nextScheduled = now + 300_000L; // +5m
            producerService.sendRetry(KafkaConfig.TOPIC_RETRY_5M, payload.withRetryCount(nextAttempt), nextScheduled);
            log.info("Routed delivery {} to retry-5m (attempt 2)", payload.deliveryId());
        } else if (currentAttempt == 2) {
            int nextAttempt = 3;
            transactionTemplate.executeWithoutResult(status ->
                deliveryRepository.updateStatusAndRetryCount(payload.deliveryId(), DeliveryStatus.RETRYING, nextAttempt)
            );
            long nextScheduled = now + 900_000L; // +15m
            producerService.sendRetry(KafkaConfig.TOPIC_RETRY_15M, payload.withRetryCount(nextAttempt), nextScheduled);
            log.info("Routed delivery {} to retry-15m (attempt 3)", payload.deliveryId());
        } else {
            // Final failure: route to DLQ
            transactionTemplate.executeWithoutResult(status ->
                deliveryRepository.updateStatus(payload.deliveryId(), DeliveryStatus.DEAD_LETTER)
            );
            producerService.sendDlq(payload, errorMessage, statusCode);
            log.warn("Exhausted retries for delivery {}. Routed to Dead-Letter Queue (DLQ).", payload.deliveryId());
        }
    }

    private RestClient getOrCreateRestClient(String targetEndpoint) {
        String baseHost = extractHost(targetEndpoint);
        return restClients.computeIfAbsent(baseHost, k -> restClientBuilder.build());
    }

    public static String extractHost(String endpoint) {
        try {
            URI uri = URI.create(endpoint);
            String host = uri.getHost();
            return host != null ? host : "unknown-host";
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}
