package com.hookshield.service;

import com.hookshield.config.KafkaConfig;
import com.hookshield.domain.record.WebhookIngestPayload;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * High-throughput Kafka publisher for fast ingestion dispatch and retry management.
 */
@Service
public class WebhookProducerService {

    private static final Logger log = LoggerFactory.getLogger(WebhookProducerService.class);

    private final KafkaTemplate<String, WebhookIngestPayload> kafkaTemplate;

    public WebhookProducerService(KafkaTemplate<String, WebhookIngestPayload> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Dispatches payload to webhook-inbound-raw.
     * Partition Key: {tenantId}:{provider}:{externalEventId} ensures strict per-entity ordering.
     */
    public CompletableFuture<SendResult<String, WebhookIngestPayload>> sendInboundRaw(
            WebhookIngestPayload payload, boolean isReplay) {

        String partitionKey = buildPartitionKey(
            payload.tenantId().toString(),
            payload.provider(),
            payload.externalEventId()
        );

        ProducerRecord<String, WebhookIngestPayload> record =
            new ProducerRecord<>(KafkaConfig.TOPIC_INBOUND_RAW, partitionKey, payload);

        record.headers().add(new RecordHeader(
            "X-HookShield-Delivery-ID",
            payload.deliveryId().toString().getBytes(StandardCharsets.UTF_8)
        ));

        if (isReplay) {
            record.headers().add(new RecordHeader(
                "X-HookShield-Replay",
                "true".getBytes(StandardCharsets.UTF_8)
            ));
        }

        return kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish webhook to topic {}: {}", KafkaConfig.TOPIC_INBOUND_RAW, ex.getMessage());
            } else {
                log.debug("Published webhook {} to partition {} with offset {}",
                    payload.deliveryId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
    }

    public CompletableFuture<SendResult<String, WebhookIngestPayload>> sendDispatchQueue(WebhookIngestPayload payload) {
        String partitionKey = buildPartitionKey(
            payload.tenantId().toString(),
            payload.provider(),
            payload.externalEventId()
        );

        ProducerRecord<String, WebhookIngestPayload> record =
            new ProducerRecord<>(KafkaConfig.TOPIC_DISPATCH_QUEUE, partitionKey, payload);

        return kafkaTemplate.send(record);
    }

    public CompletableFuture<SendResult<String, WebhookIngestPayload>> sendRetry(
            String topicName, WebhookIngestPayload payload, long nextScheduledEpochMs) {

        String partitionKey = buildPartitionKey(
            payload.tenantId().toString(),
            payload.provider(),
            payload.externalEventId()
        );

        ProducerRecord<String, WebhookIngestPayload> record =
            new ProducerRecord<>(topicName, partitionKey, payload);

        record.headers().add(new RecordHeader(
            "X-Retry-Attempt",
            String.valueOf(payload.retryCount()).getBytes(StandardCharsets.UTF_8)
        ));
        record.headers().add(new RecordHeader(
            "X-Retry-Next-Epoch-Ms",
            String.valueOf(nextScheduledEpochMs).getBytes(StandardCharsets.UTF_8)
        ));

        return kafkaTemplate.send(record);
    }

    public CompletableFuture<SendResult<String, WebhookIngestPayload>> sendDlq(
            WebhookIngestPayload payload, String errorMessage, int statusCode) {

        String partitionKey = buildPartitionKey(
            payload.tenantId().toString(),
            payload.provider(),
            payload.externalEventId()
        );

        ProducerRecord<String, WebhookIngestPayload> record =
            new ProducerRecord<>(KafkaConfig.TOPIC_DEAD_LETTER_QUEUE, partitionKey, payload);

        if (errorMessage != null) {
            record.headers().add(new RecordHeader(
                "X-Error-Message",
                errorMessage.getBytes(StandardCharsets.UTF_8)
            ));
        }
        record.headers().add(new RecordHeader(
            "X-Http-Status-Code",
            String.valueOf(statusCode).getBytes(StandardCharsets.UTF_8)
        ));

        return kafkaTemplate.send(record);
    }

    public static String buildPartitionKey(String tenantId, String provider, String externalEventId) {
        return (tenantId != null ? tenantId : "") + ":" +
            (provider != null ? provider : "") + ":" +
            (externalEventId != null ? externalEventId : "default");
    }
}
