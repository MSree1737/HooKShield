package com.hookshield.repository;

import com.hookshield.domain.entity.DlqIncidentReport;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface DlqIncidentReportRepository extends JpaRepository<DlqIncidentReport, UUID> {

    @Query("SELECT r FROM DlqIncidentReport r WHERE r.cluster.id = :clusterId")
    Page<DlqIncidentReport> findByClusterId(@Param("clusterId") UUID clusterId, Pageable pageable);

    @Query("SELECT r FROM DlqIncidentReport r WHERE r.delivery.id = :deliveryId")
    Optional<DlqIncidentReport> findByDeliveryId(@Param("deliveryId") UUID deliveryId);

    long countByClusterId(UUID clusterId);

    List<DlqIncidentReport> findByClusterIdAndResolvedFalse(UUID clusterId);
}
