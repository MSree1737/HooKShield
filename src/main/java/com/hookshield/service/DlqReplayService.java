package com.hookshield.service;

import com.hookshield.domain.entity.DlqIncidentReport;
import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.DeliveryStatus;
import com.hookshield.domain.record.ReplayResponse;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.repository.DlqIncidentReportRepository;
import com.hookshield.repository.WebhookDeliveryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

/**
 * Enterprise DLQ replay service supporting selective and bulk replays by clusterId,
 * tenantId, or explicit delivery UUIDs.
 */
@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);

    private final WebhookDeliveryRepository deliveryRepository;
    private final DlqIncidentReportRepository incidentReportRepository;
    private final WebhookProducerService producerService;
    private final IdempotencyService idempotencyService;

    public DlqReplayService(WebhookDeliveryRepository deliveryRepository,
                            DlqIncidentReportRepository incidentReportRepository,
                            WebhookProducerService producerService,
                            IdempotencyService idempotencyService) {
        this.deliveryRepository = deliveryRepository;
        this.incidentReportRepository = incidentReportRepository;
        this.producerService = producerService;
        this.idempotencyService = idempotencyService;
    }

    /**
     * Executes bulk replay of failed webhooks.
     * Transitions status to REPLAYED, clears idempotency locks,
     * and republishes to webhook-inbound-raw with header X-HookShield-Replay: true.
     */
    @Transactional
    public ReplayResponse replayDeliveries(UUID clusterId, UUID tenantId, List<UUID> deliveryIds) {
        List<WebhookDelivery> targets = new ArrayList<>();

        if (deliveryIds != null && !deliveryIds.isEmpty()) {
            targets.addAll(deliveryRepository.findAllById(deliveryIds));
        } else if (clusterId != null) {
            targets.addAll(deliveryRepository.findDeadLetterDeliveriesByClusterId(clusterId));
        } else if (tenantId != null) {
            targets.addAll(deliveryRepository.findByTenantAndStatusAndTimeRange(
                tenantId,
                DeliveryStatus.DEAD_LETTER,
                Instant.EPOCH,
                Instant.now()
            ));
        }

        if (targets.isEmpty()) {
            return new ReplayResponse(0, "No dead-lettered webhooks found matching the replay criteria", List.of());
        }

        List<UUID> replayedIds = new ArrayList<>();

        for (WebhookDelivery delivery : targets) {
            try {
                // 1. Release or reset idempotency lock so re-ingested message can proceed
                if (delivery.getIdempotencyHash() != null) {
                    idempotencyService.releaseLock(delivery.getIdempotencyHash());
                }

                // 2. Update status in database
                delivery.setStatus(DeliveryStatus.REPLAYED);
                delivery.setRetryCount(0);
                deliveryRepository.save(delivery);

                // 3. Resolve associated incident reports
                Optional<DlqIncidentReport> reportOpt = incidentReportRepository.findByDeliveryId(delivery.getId());
                reportOpt.ifPresent(report -> {
                    report.setResolved(true);
                    incidentReportRepository.save(report);
                });

                // 4. Construct payload and republish to Kafka
                WebhookIngestPayload replayPayload = new WebhookIngestPayload(
                    delivery.getId(),
                    delivery.getTenant().getId(),
                    delivery.getProvider(),
                    delivery.getExternalEventId(),
                    delivery.getPayload(),
                    delivery.getTargetEndpoint(),
                    Map.of("X-HookShield-Replay", "true"),
                    0,
                    Instant.now()
                );

                producerService.sendInboundRaw(replayPayload, true);
                replayedIds.add(delivery.getId());
                log.info("Successfully replayed dead-lettered webhook {}", delivery.getId());
            } catch (Exception e) {
                log.error("Failed to replay webhook {}: {}", delivery.getId(), e.getMessage());
            }
        }

        return new ReplayResponse(
            replayedIds.size(),
            "Successfully triggered replay for " + replayedIds.size() + " webhooks",
            replayedIds
        );
    }
}
