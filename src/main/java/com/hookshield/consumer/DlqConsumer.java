package com.hookshield.consumer;

import com.hookshield.config.KafkaConfig;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.service.AiDiagnosticService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Dead-Letter Queue consumer that triggers Spring AI & pgvector semantic analysis,
 * failure diagnosis classification, and automated replay workflows.
 */
@Component
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    private final AiDiagnosticService aiDiagnosticService;

    public DlqConsumer(AiDiagnosticService aiDiagnosticService) {
        this.aiDiagnosticService = aiDiagnosticService;
    }

    @KafkaListener(
        topics = KafkaConfig.TOPIC_DEAD_LETTER_QUEUE,
        groupId = KafkaConfig.CONSUMER_GROUP_DLQ,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processDlq(@Payload WebhookIngestPayload payload,
                           @Header(name = "X-Error-Message", required = false) byte[] errorBytes,
                           @Header(name = "X-Http-Status-Code", required = false) byte[] statusCodeBytes,
                           Acknowledgment ack) {
        try {
            String errorMessage = errorBytes != null ? new String(errorBytes, StandardCharsets.UTF_8) : "Unknown Error";
            Integer statusCode = null;

            if (statusCodeBytes != null) {
                try {
                    statusCode = Integer.parseInt(new String(statusCodeBytes, StandardCharsets.UTF_8));
                } catch (NumberFormatException ignored) {}
            }

            log.warn("DLQ Consumer received dead-lettered delivery {}. Starting AI root-cause diagnosis.", payload.deliveryId());
            aiDiagnosticService.triageFailure(payload, errorMessage, statusCode);
        } catch (Exception e) {
            log.error("Failed to process DLQ event for delivery {}: {}", payload.deliveryId(), e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }
}
