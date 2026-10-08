package com.hookshield.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private IdempotencyService idempotencyService;

    @BeforeEach
    void setUp() {
        lenient().when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        idempotencyService = new IdempotencyService(stringRedisTemplate);
    }

    @Test
    @DisplayName("Should generate deterministic SHA-256 hash for identical inputs")
    void testDeterministicHashComputation() {
        UUID tenantId = UUID.randomUUID();
        String hash1 = idempotencyService.computeHash(tenantId, "STRIPE", "{\"amount\":100}", "evt_123");
        String hash2 = idempotencyService.computeHash(tenantId, "STRIPE", "{\"amount\":100}", "evt_123");

        assertThat(hash1).isEqualTo(hash2);
        assertThat(hash1).hasSize(64); // standard sha-256 hex length
    }

    @Test
    @DisplayName("Should acquire lock on first occurrence of message hash")
    void testAcquireLockNewMessage() {
        when(valueOperations.setIfAbsent(anyString(), eq("PROCESSING"), any(Duration.class))).thenReturn(Boolean.TRUE);

        boolean acquired = idempotencyService.acquireIdempotencyLock("hash_abc_123");
        assertThat(acquired).isTrue();
    }

    @Test
    @DisplayName("Should reject lock when message hash already exists (duplicate detected)")
    void testRejectLockDuplicateMessage() {
        when(valueOperations.setIfAbsent(anyString(), eq("PROCESSING"), any(Duration.class))).thenReturn(Boolean.FALSE);

        boolean acquired = idempotencyService.acquireIdempotencyLock("hash_abc_123");
        assertThat(acquired).isFalse();
    }

    @Test
    @DisplayName("Should release lock upon failure or replay")
    void testReleaseLock() {
        idempotencyService.releaseLock("hash_abc_123");
        verify(stringRedisTemplate).delete("idempotency:hash_abc_123");
    }
}
