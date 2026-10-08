package com.hookshield.controller;

import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.service.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(WebhookIngressController.class)
class WebhookIngressControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TenantSecretCacheService secretCacheService;

    @MockBean
    private RateLimitingService rateLimitingService;

    @MockBean
    private WebhookProducerService producerService;

    @MockBean
    private MetricsService metricsService;

    private static final String SECRET = "whsec_test_secret_12345";
    private static final String PAYLOAD = "{\"event\":\"charge.succeeded\",\"amount\":1000}";

    @Test
    @DisplayName("Should accept valid webhook, return HTTP 202 Accepted and X-HookShield-Delivery-ID header")
    void testSuccessfulIngestionFastAck() throws Exception {
        UUID tenantId = UUID.randomUUID();
        String provider = "stripe";
        String validSignature = HmacSignatureVerifier.computeHmacSha256Hex(PAYLOAD, SECRET);

        TenantSecretCacheService.CachedProviderConfig config = new TenantSecretCacheService.CachedProviderConfig(
            tenantId, "STRIPE", SECRET, "Stripe-Signature", 500, 1000
        );

        when(secretCacheService.getConfig(tenantId, provider)).thenReturn(Optional.of(config));
        when(rateLimitingService.tryAcquire(eq(tenantId), anyInt(), anyInt())).thenReturn(true);
        when(producerService.sendInboundRaw(any(WebhookIngestPayload.class), eq(false)))
            .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        mockMvc.perform(post("/api/v1/webhooks/ingest/{tenantId}/{provider}", tenantId, provider)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Stripe-Signature", validSignature)
                .header("X-Event-ID", "evt_stripe_100")
                .content(PAYLOAD))
            .andExpect(status().isAccepted())
            .andExpect(header().exists("X-HookShield-Delivery-ID"))
            .andExpect(jsonPath("$.status").value("ACCEPTED"))
            .andExpect(jsonPath("$.deliveryId").exists());

        verify(producerService).sendInboundRaw(any(WebhookIngestPayload.class), eq(false));
        verify(metricsService).incrementIngested(tenantId.toString(), provider);
    }

    @Test
    @DisplayName("Should return HTTP 429 when tenant rate limit is breached")
    void testRateLimitBreached() throws Exception {
        UUID tenantId = UUID.randomUUID();
        String provider = "stripe";

        TenantSecretCacheService.CachedProviderConfig config = new TenantSecretCacheService.CachedProviderConfig(
            tenantId, "STRIPE", SECRET, "Stripe-Signature", 500, 1000
        );

        when(secretCacheService.getConfig(tenantId, provider)).thenReturn(Optional.of(config));
        when(rateLimitingService.tryAcquire(eq(tenantId), anyInt(), anyInt())).thenReturn(false);

        mockMvc.perform(post("/api/v1/webhooks/ingest/{tenantId}/{provider}", tenantId, provider)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Stripe-Signature", "sig")
                .content(PAYLOAD))
            .andExpect(status().isTooManyRequests())
            .andExpect(header().string("Retry-After", "1"))
            .andExpect(jsonPath("$.error").value("Rate limit quota exceeded for tenant"));

        verifyNoInteractions(producerService);
    }

    @Test
    @DisplayName("Should return HTTP 401 when signature verification fails")
    void testInvalidSignature() throws Exception {
        UUID tenantId = UUID.randomUUID();
        String provider = "stripe";

        TenantSecretCacheService.CachedProviderConfig config = new TenantSecretCacheService.CachedProviderConfig(
            tenantId, "STRIPE", SECRET, "Stripe-Signature", 500, 1000
        );

        when(secretCacheService.getConfig(tenantId, provider)).thenReturn(Optional.of(config));
        when(rateLimitingService.tryAcquire(eq(tenantId), anyInt(), anyInt())).thenReturn(true);

        mockMvc.perform(post("/api/v1/webhooks/ingest/{tenantId}/{provider}", tenantId, provider)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Stripe-Signature", "invalid_bad_sig_hex_123")
                .content(PAYLOAD))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.error").value("Invalid cryptographic signature"));

        verifyNoInteractions(producerService);
    }

    @Test
    @DisplayName("Should return HTTP 401 when tenant provider configuration is missing")
    void testMissingTenantConfig() throws Exception {
        UUID tenantId = UUID.randomUUID();
        String provider = "unknown_provider";

        when(secretCacheService.getConfig(tenantId, provider)).thenReturn(Optional.empty());

        mockMvc.perform(post("/api/v1/webhooks/ingest/{tenantId}/{provider}", tenantId, provider)
                .contentType(MediaType.APPLICATION_JSON)
                .content(PAYLOAD))
            .andExpect(status().isUnauthorized());

        verifyNoInteractions(producerService);
    }
}
