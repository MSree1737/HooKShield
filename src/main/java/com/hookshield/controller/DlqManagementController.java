package com.hookshield.controller;

import com.hookshield.domain.entity.DlqIncidentReport;
import com.hookshield.domain.entity.ErrorCluster;
import com.hookshield.domain.record.*;
import com.hookshield.repository.DlqIncidentReportRepository;
import com.hookshield.repository.ErrorClusterRepository;
import com.hookshield.service.DlqReplayService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST controller for Dead-Letter Queue management, cluster inspection, and bulk replay.
 */
@RestController
@RequestMapping("/api/v1/dlq")
@CrossOrigin(origins = "*")
public class DlqManagementController {

    private final ErrorClusterRepository clusterRepository;
    private final DlqIncidentReportRepository incidentReportRepository;
    private final DlqReplayService replayService;

    public DlqManagementController(ErrorClusterRepository clusterRepository,
                                   DlqIncidentReportRepository incidentReportRepository,
                                   DlqReplayService replayService) {
        this.clusterRepository = clusterRepository;
        this.incidentReportRepository = incidentReportRepository;
        this.replayService = replayService;
    }

    /**
     * Retrieve aggregated semantic error clusters with occurrence counts and AI categorization.
     */
    @GetMapping("/clusters")
    public ResponseEntity<List<ClusterSummaryResponse>> listClusters() {
        List<ErrorCluster> clusters = clusterRepository.findAll(Sort.by(Sort.Direction.DESC, "totalOccurrences"));
        List<ClusterSummaryResponse> responses = clusters.stream()
            .map(c -> new ClusterSummaryResponse(
                c.getId(),
                c.getClusterName(),
                c.getFailureCategory(),
                c.getTotalOccurrences(),
                c.isSafeToAutoReplay(),
                c.getRecommendedAction(),
                c.getCreatedAt(),
                c.getUpdatedAt()
            ))
            .toList();

        return ResponseEntity.ok(responses);
    }

    /**
     * Paginated search of individual failed deliveries within an error cluster.
     */
    @GetMapping("/incidents")
    public ResponseEntity<Page<IncidentResponse>> listIncidents(
            @RequestParam UUID clusterId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        PageRequest pageRequest = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<DlqIncidentReport> reports = incidentReportRepository.findByClusterId(clusterId, pageRequest);

        Page<IncidentResponse> responsePage = reports.map(r -> new IncidentResponse(
            r.getId(),
            r.getDelivery().getId(),
            r.getCluster() != null ? r.getCluster().getId() : null,
            r.getHttpStatusCode(),
            r.getRawErrorMessage(),
            r.getAiRootCauseAnalysis(),
            r.getAiConfidenceScore(),
            r.isResolved(),
            r.getCreatedAt()
        ));

        return ResponseEntity.ok(responsePage);
    }

    /**
     * Replay dead-lettered webhooks by clusterId, tenantId, or explicit list of delivery IDs.
     */
    @PostMapping("/replay")
    public ResponseEntity<ReplayResponse> replayWebhooks(@RequestBody ReplayRequest request) {
        ReplayResponse response = replayService.replayDeliveries(
            request.clusterId(),
            request.tenantId(),
            request.deliveryIds()
        );

        return ResponseEntity.ok(response);
    }
}
