package com.hookshield.controller;

import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.domain.record.WebhookIngestResponse;
import com.hookshield.service.*;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;

/**
 * Ultra-low latency (<15ms) ingestion gateway controller running on Java 21 Virtual Threads.
 * Performs HMAC signature verification, Redis-backed rate limiting, and fast-ack Kafka dispatch.
 */
@RestController
@RequestMapping("/api/v1/webhooks")
@CrossOrigin(origins = "*")
public class WebhookIngressController {

    private static final Logger log = LoggerFactory.getLogger(WebhookIngressController.class);

    private final TenantSecretCacheService secretCacheService;
    private final RateLimitingService rateLimitingService;
    private final WebhookProducerService producerService;
    private final MetricsService metricsService;

    public WebhookIngressController(TenantSecretCacheService secretCacheService,
                                    RateLimitingService rateLimitingService,
                                    WebhookProducerService producerService,
                                    MetricsService metricsService) {
        this.secretCacheService = secretCacheService;
        this.rateLimitingService = rateLimitingService;
        this.producerService = producerService;
        this.metricsService = metricsService;
    }

    @PostMapping(
        value = "/ingest/{tenantId}/{provider}",
        consumes = MediaType.ALL_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<?> ingestWebhook(
            @PathVariable UUID tenantId,
            @PathVariable String provider,
            @RequestBody(required = false) String rawPayload,
            @RequestParam(required = false) String targetEndpoint,
            HttpServletRequest request) {

        long startTime = System.nanoTime();

        // 1. Retrieve tenant provider configuration from Redis cache (with PostgreSQL fallback)
        Optional<TenantSecretCacheService.CachedProviderConfig> configOpt =
            secretCacheService.getConfig(tenantId, provider);

        if (configOpt.isEmpty()) {
            log.warn("Ingestion rejected: No active configuration for tenant {} and provider {}", tenantId, provider);
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.of("error", "Tenant or provider configuration not found or inactive"));
        }

        TenantSecretCacheService.CachedProviderConfig config = configOpt.get();

        // 2. High-Throughput Token Bucket Rate Limiting
        boolean withinQuota = rateLimitingService.tryAcquire(
            tenantId,
            config.rateLimitPerSecond(),
            config.burstCapacity()
        );

        if (!withinQuota) {
            log.warn("Tenant {} exceeded configured rate limit ({} req/s, burst {})",
                tenantId, config.rateLimitPerSecond(), config.burstCapacity());
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(Map.of("error", "Rate limit quota exceeded for tenant"));
        }

        // 3. Cryptographic HMAC-SHA256 Signature Verification
        String signatureHeaderName = config.headerSignatureKey();
        String signatureHeaderValue = request.getHeader(signatureHeaderName);

        // Fallback checks for standard headers if header name differs slightly
        if (signatureHeaderValue == null) {
            signatureHeaderValue = request.getHeader("X-Hub-Signature-256");
        }
        if (signatureHeaderValue == null) {
            signatureHeaderValue = request.getHeader("Stripe-Signature");
        }
        if (signatureHeaderValue == null) {
            signatureHeaderValue = request.getHeader("X-Signature");
        }

        String payloadContent = rawPayload != null ? rawPayload : "";
        boolean validSignature = HmacSignatureVerifier.verify(
            payloadContent,
            config.hmacSecret(),
            signatureHeaderValue
        );

        if (!validSignature) {
            log.warn("HMAC verification failed for tenant {} and provider {}. Header: {}",
                tenantId, provider, signatureHeaderName);
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.of("error", "Invalid cryptographic signature"));
        }

        // 4. Extract external event ID and destination target
        String externalEventId = request.getHeader("X-Event-ID");
        if (externalEventId == null || externalEventId.isBlank()) {
            externalEventId = request.getHeader("X-GitHub-Delivery");
        }
        if (externalEventId == null || externalEventId.isBlank()) {
            externalEventId = UUID.randomUUID().toString();
        }

        String resolvedTargetEndpoint = targetEndpoint != null && !targetEndpoint.isBlank()
            ? targetEndpoint
            : request.getHeader("X-HookShield-Target-Endpoint");

        if (resolvedTargetEndpoint == null || resolvedTargetEndpoint.isBlank()) {
            resolvedTargetEndpoint = "http://localhost:8080/api/v1/webhooks/mock-subscriber";
        }

        // Collect request headers
        Map<String, String> forwardedHeaders = new HashMap<>();
        Enumeration<String> headerNames = request.getHeaderNames();
        if (headerNames != null) {
            while (headerNames.hasMoreElements()) {
                String header = headerNames.nextElement();
                forwardedHeaders.put(header, request.getHeader(header));
            }
        }

        UUID deliveryId = UUID.randomUUID();

        WebhookIngestPayload ingestPayload = new WebhookIngestPayload(
            deliveryId,
            tenantId,
            provider,
            externalEventId,
            payloadContent,
            resolvedTargetEndpoint,
            forwardedHeaders,
            0,
            Instant.now()
        );

        // 5. Asynchronous Fast-Ack to Kafka
        producerService.sendInboundRaw(ingestPayload, false);

        // 6. Record metrics and return 202 Accepted (<15ms SLA target)
        metricsService.incrementIngested(tenantId.toString(), provider);

        long latencyMillis = (System.nanoTime() - startTime) / 1_000_000;
        log.debug("Webhook {} ingested successfully in {} ms", deliveryId, latencyMillis);

        return ResponseEntity.status(HttpStatus.ACCEPTED)
            .header("X-HookShield-Delivery-ID", deliveryId.toString())
            .header("X-Ingestion-Latency-Ms", String.valueOf(latencyMillis))
            .body(WebhookIngestResponse.accepted(deliveryId));
    }

    /**
     * Built-in mock subscriber endpoint for testing outbound dispatch, retries, and DLQ pipelines.
     * Supports optional status code overrides via ?status=503 or header X-Mock-Status: 503.
     */
    @PostMapping(
        value = "/mock-subscriber",
        consumes = MediaType.ALL_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<?> mockSubscriber(
            @RequestBody(required = false) String payload,
            @RequestParam(name = "status", required = false) Integer statusCodeParam,
            @RequestHeader(name = "X-Mock-Status", required = false) Integer statusCodeHeader) {
        int status = statusCodeParam != null ? statusCodeParam : (statusCodeHeader != null ? statusCodeHeader : 200);
        log.info("Mock subscriber received dispatch: status={}, payload length={}", status, payload != null ? payload.length() : 0);
        if (status >= 400) {
            return ResponseEntity.status(status).body(Map.of("error", "Simulated subscriber failure", "statusCode", status));
        }
        return ResponseEntity.ok(Map.of("status", "success", "received", true));
    }
}
