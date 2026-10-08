package com.hookshield.domain.record;

import java.util.List;
import java.util.UUID;

/**
 * Tenant and configured providers with rate-limits and keys.
 */
public record TenantConfigResponse(
    UUID id,
    String name,
    int rateLimitPerSecond,
    int burstCapacity,
    List<ProviderConfigItem> providers
) {
    public record ProviderConfigItem(
        UUID id,
        String providerName,
        String headerSignatureKey,
        String maskedSecret,
        boolean active
    ) {}
}
