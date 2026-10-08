package com.hookshield.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RateLimitingServiceTest {

    @Mock
    private RedissonClient redissonClient;

    @Mock
    private RRateLimiter rateLimiter;

    private RateLimitingService rateLimitingService;

    @BeforeEach
    void setUp() {
        rateLimitingService = new RateLimitingService(redissonClient);
    }

    @Test
    @DisplayName("Should allow request when tokens are available")
    void testAcquireTokenWithinLimit() {
        UUID tenantId = UUID.randomUUID();
        when(redissonClient.getRateLimiter(anyString())).thenReturn(rateLimiter);
        when(rateLimiter.trySetRate(any(RateType.class), anyLong(), any(Duration.class))).thenReturn(true);
        when(rateLimiter.tryAcquire(1)).thenReturn(true);

        boolean allowed = rateLimitingService.tryAcquire(tenantId, 500, 1000);

        assertThat(allowed).isTrue();
        verify(rateLimiter).tryAcquire(1);
    }

    @Test
    @DisplayName("Should reject request when token bucket quota is exhausted")
    void testRejectWhenRateLimitBreached() {
        UUID tenantId = UUID.randomUUID();
        when(redissonClient.getRateLimiter(anyString())).thenReturn(rateLimiter);
        when(rateLimiter.trySetRate(any(RateType.class), anyLong(), any(Duration.class))).thenReturn(true);
        when(rateLimiter.tryAcquire(1)).thenReturn(false);

        boolean allowed = rateLimitingService.tryAcquire(tenantId, 500, 1000);

        assertThat(allowed).isFalse();
        verify(rateLimiter).tryAcquire(1);
    }

    @Test
    @DisplayName("Should fail open on Redis failure to protect ingestion SLA")
    void testFailOpenOnRedisException() {
        UUID tenantId = UUID.randomUUID();
        when(redissonClient.getRateLimiter(anyString())).thenThrow(new RuntimeException("Redis connection timed out"));

        boolean allowed = rateLimitingService.tryAcquire(tenantId, 500, 1000);

        assertThat(allowed).isTrue();
    }
}
