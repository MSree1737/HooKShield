package com.hookshield.consumer;

import com.hookshield.config.KafkaConfig;
import com.hookshield.domain.entity.Tenant;
import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.DeliveryStatus;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.repository.TenantRepository;
import com.hookshield.repository.WebhookDeliveryRepository;
import com.hookshield.service.IdempotencyService;
import com.hookshield.service.MetricsService;
import com.hookshield.service.WebhookProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tier 2 Inbound Consumer: performs atomic Redis deduplication,
 * PostgreSQL transactional audit logging, and routes messages to the dispatch queue.
 */
@Component
public class InboundConsumer {

    private static final Logger log = LoggerFactory.getLogger(InboundConsumer.class);

    private final IdempotencyService idempotencyService;
    private final WebhookDeliveryRepository deliveryRepository;
    private final TenantRepository tenantRepository;
    private final WebhookProducerService producerService;
    private final MetricsService metricsService;

    public InboundConsumer(IdempotencyService idempotencyService,
                           WebhookDeliveryRepository deliveryRepository,
                           TenantRepository tenantRepository,
                           WebhookProducerService producerService,
                           MetricsService metricsService) {
        this.idempotencyService = idempotencyService;
        this.deliveryRepository = deliveryRepository;
        this.tenantRepository = tenantRepository;
        this.producerService = producerService;
        this.metricsService = metricsService;
    }

    @KafkaListener(
        topics = KafkaConfig.TOPIC_INBOUND_RAW,
        groupId = KafkaConfig.CONSUMER_GROUP_INBOUND,
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void processInbound(@Payload WebhookIngestPayload payload, Acknowledgment ack) {
        try {
            String hash = idempotencyService.computeHash(
                payload.tenantId(),
                payload.provider(),
                payload.rawPayload(),
                payload.externalEventId()
            );

            // 1. Atomic Redis SET NX EX 86400 Deduplication Check
            boolean lockAcquired = idempotencyService.acquireIdempotencyLock(hash);
            if (!lockAcquired) {
                metricsService.incrementDeduplicated();
                log.info("Deduplication engine: Duplicate webhook ignored for deliveryId {} with hash {}",
                    payload.deliveryId(), hash);
                ack.acknowledge();
                return;
            }

            // 2. Transactional Persistence into webhook_deliveries
            Tenant tenant = tenantRepository.findById(payload.tenantId()).orElse(null);
            if (tenant == null) {
                log.error("Tenant {} not found for incoming delivery {}", payload.tenantId(), payload.deliveryId());
                idempotencyService.releaseLock(hash);
                ack.acknowledge();
                return;
            }

            WebhookDelivery delivery = deliveryRepository.findById(payload.deliveryId())
                .orElseGet(() -> {
                    WebhookDelivery d = new WebhookDelivery(
                        tenant,
                        payload.provider(),
                        payload.externalEventId(),
                        hash,
                        payload.rawPayload() != null ? payload.rawPayload() : "{}",
                        payload.targetEndpoint() != null ? payload.targetEndpoint() : ""
                    );
                    d.setId(payload.deliveryId());
                    return d;
                });

            delivery.setId(payload.deliveryId());
            delivery.setStatus(DeliveryStatus.RECEIVED);
            delivery.setIdempotencyHash(hash);
            deliveryRepository.save(delivery);

            // 3. Push to Dispatch Queue
            producerService.sendDispatchQueue(payload);
            idempotencyService.markProcessed(hash);

            log.debug("Persisted delivery {} and forwarded to dispatch queue", payload.deliveryId());
        } catch (Exception e) {
            log.error("Failed to process inbound webhook {}: {}", payload.deliveryId(), e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }
}
