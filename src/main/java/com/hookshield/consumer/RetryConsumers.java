package com.hookshield.consumer;

import com.hookshield.config.KafkaConfig;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.service.WebhookDispatcherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Non-blocking multi-tier retry consumer handling delayed backoff schedules.
 * Kafka consumer threads are never paused or put to sleep; delayed executions
 * are dispatched asynchronously via lightweight scheduler threads.
 */
@Component
public class RetryConsumers {

    private static final Logger log = LoggerFactory.getLogger(RetryConsumers.class);

    private final WebhookDispatcherService dispatcherService;
    private final ScheduledExecutorService scheduledExecutor =
        Executors.newScheduledThreadPool(4, Thread.ofVirtual().name("retry-scheduler-", 0).factory());

    public RetryConsumers(WebhookDispatcherService dispatcherService) {
        this.dispatcherService = dispatcherService;
    }

    @KafkaListener(
        topics = {
            KafkaConfig.TOPIC_RETRY_1M,
            KafkaConfig.TOPIC_RETRY_5M,
            KafkaConfig.TOPIC_RETRY_15M
        },
        groupId = KafkaConfig.CONSUMER_GROUP_RETRY,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processRetry(@Payload WebhookIngestPayload payload,
                             @Header(name = "X-Retry-Next-Epoch-Ms", required = false) byte[] nextEpochBytes,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                             Acknowledgment ack) {
        try {
            long now = Instant.now().toEpochMilli();
            long nextScheduledEpoch = now;

            if (nextEpochBytes != null) {
                try {
                    nextScheduledEpoch = Long.parseLong(new String(nextEpochBytes, StandardCharsets.UTF_8));
                } catch (NumberFormatException ignored) {}
            }

            long delayRemainingMs = Math.max(0, nextScheduledEpoch - now);

            if (delayRemainingMs <= 0) {
                log.info("Executing immediate retry for delivery {} from topic {}", payload.deliveryId(), topic);
                dispatcherService.dispatch(payload);
            } else {
                log.info("Scheduling non-blocking delayed retry for delivery {} in {} ms from topic {}",
                    payload.deliveryId(), delayRemainingMs, topic);

                // Asynchronous non-blocking dispatch: consumer thread is immediately freed and offset committed
                scheduledExecutor.schedule(() -> {
                    try {
                        dispatcherService.dispatch(payload);
                    } catch (Exception e) {
                        log.error("Error during scheduled retry dispatch for {}: {}", payload.deliveryId(), e.getMessage());
                    }
                }, delayRemainingMs, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            log.error("Error handling retry topic message for {}: {}", payload.deliveryId(), e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }
}
