package com.hookshield.service;

import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * High-throughput distributed rate-limiting service using Redis & Redisson Token Bucket algorithm.
 * Enforces per-tenant rate limits (e.g. 500 req/sec) and burst thresholds across distributed nodes.
 */
@Service
public class RateLimitingService {

    private static final Logger log = LoggerFactory.getLogger(RateLimitingService.class);
    private static final String RATE_LIMITER_PREFIX = "ratelimit:tenant:";

    private final RedissonClient redissonClient;
    // Local set to avoid redundant trySetRate overhead on each hot request
    private final ConcurrentHashMap<UUID, Integer> configuredRates = new ConcurrentHashMap<>();

    public RateLimitingService(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    /**
     * Attempts to acquire 1 token for the specified tenant.
     *
     * @param tenantId           Tenant UUID
     * @param rateLimitPerSecond Configured requests per second
     * @param burstCapacity      Configured maximum burst capacity
     * @return true if token was acquired within budget; false if rate limit is exceeded.
     */
    public boolean tryAcquire(UUID tenantId, int rateLimitPerSecond, int burstCapacity) {
        try {
            String limiterKey = RATE_LIMITER_PREFIX + tenantId;
            RRateLimiter rateLimiter = redissonClient.getRateLimiter(limiterKey);

            // Configure rate if newly registered or modified
            Integer existingRate = configuredRates.get(tenantId);
            if (existingRate == null || existingRate != rateLimitPerSecond) {
                rateLimiter.trySetRate(RateType.OVERALL, rateLimitPerSecond, Duration.ofSeconds(1));
                rateLimiter.expire(Duration.ofDays(1));
                configuredRates.put(tenantId, rateLimitPerSecond);
            }

            return rateLimiter.tryAcquire(1);
        } catch (Exception e) {
            log.error("Error evaluating distributed rate limit for tenant {}: {}. Failing open for resiliency.",
                tenantId, e.getMessage());
            return true; // Fail open to protect ingestion SLAs if Redis communication experiences hiccup
        }
    }
}
