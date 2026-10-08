package com.hookshield.repository;

import com.hookshield.domain.entity.TenantProviderConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TenantProviderConfigRepository extends JpaRepository<TenantProviderConfig, UUID> {

    @Query("SELECT c FROM TenantProviderConfig c WHERE c.tenant.id = :tenantId AND UPPER(c.providerName) = UPPER(:providerName) AND c.isActive = true")
    Optional<TenantProviderConfig> findActiveConfig(@Param("tenantId") UUID tenantId, @Param("providerName") String providerName);

    @Query("SELECT c FROM TenantProviderConfig c JOIN FETCH c.tenant WHERE c.tenant.id = :tenantId AND UPPER(c.providerName) = UPPER(:providerName)")
    Optional<TenantProviderConfig> findByTenantIdAndProviderNameIgnoreCase(@Param("tenantId") UUID tenantId, @Param("providerName") String providerName);

    @Query("SELECT c FROM TenantProviderConfig c JOIN FETCH c.tenant WHERE c.isActive = true ORDER BY c.tenant.name ASC, c.providerName ASC")
    List<TenantProviderConfig> findAllActiveWithTenant();
}
