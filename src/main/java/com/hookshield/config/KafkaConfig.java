package com.hookshield.config;

import com.hookshield.domain.record.WebhookIngestPayload;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

@EnableKafka
@Configuration
public class KafkaConfig {

    public static final String TOPIC_INBOUND_RAW = "webhook-inbound-raw";
    public static final String TOPIC_DISPATCH_QUEUE = "webhook-dispatch-queue";
    public static final String TOPIC_RETRY_1M = "webhook-retry-1m";
    public static final String TOPIC_RETRY_5M = "webhook-retry-5m";
    public static final String TOPIC_RETRY_15M = "webhook-retry-15m";
    public static final String TOPIC_DEAD_LETTER_QUEUE = "webhook-dead-letter-queue";

    public static final String CONSUMER_GROUP_INBOUND = "hookshield-inbound-group";
    public static final String CONSUMER_GROUP_DISPATCH = "hookshield-dispatch-group";
    public static final String CONSUMER_GROUP_RETRY = "hookshield-retry-group";
    public static final String CONSUMER_GROUP_DLQ = "hookshield-dlq-group";

    @Value("${spring.kafka.bootstrap-servers:localhost:9094}")
    private String bootstrapServers;

    @Bean
    @org.springframework.context.annotation.Profile("!test")
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setFatalIfBrokerNotAvailable(false);
        return admin;
    }

    // --- Topic Declarations ---
    @Bean
    @org.springframework.context.annotation.Profile("!test")
    public NewTopic inboundRawTopic() {
        return TopicBuilder.name(TOPIC_INBOUND_RAW)
            .partitions(12)
            .replicas(1)
            .build();
    }

    @Bean
    @org.springframework.context.annotation.Profile("!test")
    public NewTopic dispatchQueueTopic() {
        return TopicBuilder.name(TOPIC_DISPATCH_QUEUE)
            .partitions(12)
            .replicas(1)
            .build();
    }

    @Bean
    @org.springframework.context.annotation.Profile("!test")
    public NewTopic retry1mTopic() {
        return TopicBuilder.name(TOPIC_RETRY_1M)
            .partitions(6)
            .replicas(1)
            .build();
    }

    @Bean
    @org.springframework.context.annotation.Profile("!test")
    public NewTopic retry5mTopic() {
        return TopicBuilder.name(TOPIC_RETRY_5M)
            .partitions(6)
            .replicas(1)
            .build();
    }

    @Bean
    @org.springframework.context.annotation.Profile("!test")
    public NewTopic retry15mTopic() {
        return TopicBuilder.name(TOPIC_RETRY_15M)
            .partitions(6)
            .replicas(1)
            .build();
    }

    @Bean
    @org.springframework.context.annotation.Profile("!test")
    public NewTopic dlqTopic() {
        return TopicBuilder.name(TOPIC_DEAD_LETTER_QUEUE)
            .partitions(6)
            .replicas(1)
            .build();
    }

    // --- Producer Configuration ---
    @Bean
    public ProducerFactory<String, WebhookIngestPayload> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, WebhookIngestPayload> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // --- Consumer Configuration ---
    @Bean
    public ConsumerFactory<String, WebhookIngestPayload> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_INBOUND);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        JsonDeserializer<WebhookIngestPayload> jsonDeserializer = new JsonDeserializer<>(WebhookIngestPayload.class);
        jsonDeserializer.addTrustedPackages("com.hookshield.*", "*");
        jsonDeserializer.setUseTypeHeaders(false);

        ErrorHandlingDeserializer<WebhookIngestPayload> errorDeserializer =
            new ErrorHandlingDeserializer<>(jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            errorDeserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WebhookIngestPayload> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, WebhookIngestPayload> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Dispatch listener executions to Project Loom Virtual Threads
        factory.getContainerProperties().setListenerTaskExecutor(
            new org.springframework.core.task.support.TaskExecutorAdapter(
                Executors.newVirtualThreadPerTaskExecutor()
            )
        );
        return factory;
    }
}
