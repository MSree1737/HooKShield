package com.hookshield.repository;

import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.DeliveryStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface WebhookDeliveryRepository extends JpaRepository<WebhookDelivery, UUID> {

    Optional<WebhookDelivery> findByIdempotencyHash(String idempotencyHash);

    @Query(value = "SELECT d FROM WebhookDelivery d JOIN FETCH d.tenant",
           countQuery = "SELECT count(d) FROM WebhookDelivery d")
    Page<WebhookDelivery> findAllWithTenant(Pageable pageable);

    @Query(value = "SELECT d FROM WebhookDelivery d JOIN FETCH d.tenant WHERE d.status = :status",
           countQuery = "SELECT count(d) FROM WebhookDelivery d WHERE d.status = :status")
    Page<WebhookDelivery> findByStatusWithTenant(@Param("status") DeliveryStatus status, Pageable pageable);

    @Query(value = "SELECT d FROM WebhookDelivery d JOIN FETCH d.tenant WHERE d.tenant.id = :tenantId AND d.status = :status",
           countQuery = "SELECT count(d) FROM WebhookDelivery d WHERE d.tenant.id = :tenantId AND d.status = :status")
    Page<WebhookDelivery> findByTenantIdAndStatusWithTenant(@Param("tenantId") UUID tenantId, @Param("status") DeliveryStatus status, Pageable pageable);

    long countByStatus(DeliveryStatus status);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE WebhookDelivery d SET d.status = :status, d.retryCount = :retryCount, d.updatedAt = CURRENT_INSTANT WHERE d.id = :id")
    int updateStatusAndRetryCount(@Param("id") UUID id, @Param("status") DeliveryStatus status, @Param("retryCount") int retryCount);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE WebhookDelivery d SET d.status = :status, d.updatedAt = CURRENT_INSTANT WHERE d.id = :id")
    int updateStatus(@Param("id") UUID id, @Param("status") DeliveryStatus status);

    @Query("SELECT d FROM WebhookDelivery d WHERE d.tenant.id = :tenantId AND d.status = :status AND d.createdAt BETWEEN :start AND :end")
    List<WebhookDelivery> findByTenantAndStatusAndTimeRange(
        @Param("tenantId") UUID tenantId,
        @Param("status") DeliveryStatus status,
        @Param("start") Instant start,
        @Param("end") Instant end
    );

    @Query("SELECT r.delivery FROM DlqIncidentReport r WHERE r.cluster.id = :clusterId AND r.delivery.status = 'DEAD_LETTER'")
    List<WebhookDelivery> findDeadLetterDeliveriesByClusterId(@Param("clusterId") UUID clusterId);
}
