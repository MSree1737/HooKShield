package com.hookshield.domain.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "tenant_provider_configs", uniqueConstraints = {
    @UniqueConstraint(name = "uq_tenant_provider", columnNames = {"tenant_id", "provider_name"})
})
public class TenantProviderConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "tenant_id", nullable = false)
    private Tenant tenant;

    @Column(name = "provider_name", nullable = false, length = 50)
    private String providerName;

    @Column(name = "hmac_secret", nullable = false, length = 255)
    private String hmacSecret;

    @Column(name = "header_signature_key", nullable = false, length = 100)
    private String headerSignatureKey;

    @Column(name = "is_active", nullable = false)
    private boolean isActive = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    public TenantProviderConfig() {}

    public TenantProviderConfig(Tenant tenant, String providerName, String hmacSecret, String headerSignatureKey) {
        this.tenant = tenant;
        this.providerName = providerName;
        this.hmacSecret = hmacSecret;
        this.headerSignatureKey = headerSignatureKey;
        this.isActive = true;
        this.createdAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public String getHmacSecret() {
        return hmacSecret;
    }

    public void setHmacSecret(String hmacSecret) {
        this.hmacSecret = hmacSecret;
    }

    public String getHeaderSignatureKey() {
        return headerSignatureKey;
    }

    public void setHeaderSignatureKey(String headerSignatureKey) {
        this.headerSignatureKey = headerSignatureKey;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}
