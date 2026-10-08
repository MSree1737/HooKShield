package com.hookshield.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hookshield.domain.entity.Tenant;
import com.hookshield.domain.entity.TenantProviderConfig;
import com.hookshield.repository.TenantProviderConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * High-performance Redis caching service for tenant provider secrets and signature keys.
 * Minimizes database queries during sub-15ms ingestion spikes with PostgreSQL fallback.
 */
@Service
public class TenantSecretCacheService {

    private static final Logger log = LoggerFactory.getLogger(TenantSecretCacheService.class);
    private static final String CACHE_KEY_PREFIX = "tenant:provider:config:";
    private static final Duration CACHE_TTL = Duration.ofHours(1);

    private final TenantProviderConfigRepository configRepository;
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    public record CachedProviderConfig(
        UUID tenantId,
        String providerName,
        String hmacSecret,
        String headerSignatureKey,
        int rateLimitPerSecond,
        int burstCapacity
    ) {}

    public TenantSecretCacheService(TenantProviderConfigRepository configRepository,
                                    StringRedisTemplate stringRedisTemplate,
                                    ObjectMapper objectMapper) {
        this.configRepository = configRepository;
        this.stringRedisTemplate = stringRedisTemplate;
        this.objectMapper = objectMapper;
    }

    public Optional<CachedProviderConfig> getConfig(UUID tenantId, String providerName) {
        String cacheKey = CACHE_KEY_PREFIX + tenantId + ":" + providerName.toUpperCase();

        try {
            String cachedJson = stringRedisTemplate.opsForValue().get(cacheKey);
            if (cachedJson != null && !cachedJson.isBlank()) {
                return Optional.of(objectMapper.readValue(cachedJson, CachedProviderConfig.class));
            }
        } catch (Exception e) {
            log.warn("Failed to retrieve tenant provider config from Redis cache: {}", e.getMessage());
        }

        // Fallback to PostgreSQL
        Optional<TenantProviderConfig> dbConfigOpt = configRepository.findByTenantIdAndProviderNameIgnoreCase(tenantId, providerName);
        if (dbConfigOpt.isEmpty()) {
            return Optional.empty();
        }

        TenantProviderConfig dbConfig = dbConfigOpt.get();
        Tenant tenant = dbConfig.getTenant();

        CachedProviderConfig config = new CachedProviderConfig(
            tenant.getId(),
            dbConfig.getProviderName(),
            dbConfig.getHmacSecret(),
            dbConfig.getHeaderSignatureKey(),
            tenant.getRateLimitPerSecond(),
            tenant.getBurstCapacity()
        );

        try {
            String json = objectMapper.writeValueAsString(config);
            stringRedisTemplate.opsForValue().set(cacheKey, json, CACHE_TTL);
        } catch (Exception e) {
            log.warn("Failed to cache tenant provider config in Redis: {}", e.getMessage());
        }

        return Optional.of(config);
    }

    public void invalidateCache(UUID tenantId, String providerName) {
        String cacheKey = CACHE_KEY_PREFIX + tenantId + ":" + providerName.toUpperCase();
        stringRedisTemplate.delete(cacheKey);
    }
}
