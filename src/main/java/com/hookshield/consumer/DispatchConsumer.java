package com.hookshield.consumer;

import com.hookshield.config.KafkaConfig;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.service.WebhookDispatcherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Dispatch Consumer: listens on webhook-dispatch-queue and executes outbound
 * HTTP delivery calls across Project Loom Virtual Threads.
 */
@Component
public class DispatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(DispatchConsumer.class);

    private final WebhookDispatcherService dispatcherService;

    public DispatchConsumer(WebhookDispatcherService dispatcherService) {
        this.dispatcherService = dispatcherService;
    }

    @KafkaListener(
        topics = KafkaConfig.TOPIC_DISPATCH_QUEUE,
        groupId = KafkaConfig.CONSUMER_GROUP_DISPATCH,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processDispatch(@Payload WebhookIngestPayload payload, Acknowledgment ack) {
        try {
            dispatcherService.dispatch(payload);
        } catch (Exception e) {
            log.error("Unhandled exception during webhook dispatch for delivery {}: {}",
                payload.deliveryId(), e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }
}
