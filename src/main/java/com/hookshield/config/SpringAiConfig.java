package com.hookshield.config;

import com.hookshield.domain.enums.FailureCategory;
import com.hookshield.domain.record.FailureDiagnosis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.openai.OpenAiEmbeddingModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Spring AI configuration for LLM-driven triage and pgvector semantic embeddings.
 * Supports production OpenAI integrations as well as zero-downtime resilient fallbacks
 * for air-gapped or test environments.
 */
@Configuration
public class SpringAiConfig {

    private static final Logger log = LoggerFactory.getLogger(SpringAiConfig.class);
    public static final int EMBEDDING_DIMENSION = 1536;

    @Bean
    @Primary
    public ChatClient chatClient(ObjectProvider<ChatModel> chatModelProvider,
                                 @Value("${hookshield.ai.mock-enabled:false}") boolean mockEnabled,
                                 @Value("${spring.ai.openai.api-key:demo-key-for-local-dev}") String openAiApiKey) {
        boolean isPlaceholderKey = openAiApiKey == null || openAiApiKey.isBlank()
            || openAiApiKey.startsWith("demo-") || openAiApiKey.startsWith("test-");

        if (!mockEnabled && !isPlaceholderKey) {
            ChatModel chatModel = chatModelProvider.getIfAvailable();
            if (chatModel != null) {
                log.info("Configuring production Spring AI ChatClient with model: {}", chatModel.getClass().getSimpleName());
                return ChatClient.builder(chatModel).build();
            }
        }
        log.info("Configuring resilient fallback ChatClient (mockEnabled={}, placeholderKey={})", mockEnabled, isPlaceholderKey);
        return createFallbackChatClient();
    }

    @Bean
    @Primary
    public EmbeddingModel embeddingModel(ObjectProvider<OpenAiEmbeddingModel> openAiEmbeddingModelProvider,
                                         @Value("${hookshield.ai.mock-enabled:false}") boolean mockEnabled,
                                         @Value("${spring.ai.openai.api-key:demo-key-for-local-dev}") String openAiApiKey) {
        EmbeddingModel fallbackModel = createFallbackEmbeddingModel();
        boolean isPlaceholderKey = openAiApiKey == null || openAiApiKey.isBlank()
            || openAiApiKey.startsWith("demo-") || openAiApiKey.startsWith("test-");

        if (mockEnabled || isPlaceholderKey) {
            log.info("Configuring 1536-dimensional deterministic vector embedding model (mockEnabled={}, placeholderKey={})",
                mockEnabled, isPlaceholderKey);
            return fallbackModel;
        }

        OpenAiEmbeddingModel openAiModel = openAiEmbeddingModelProvider.getIfAvailable();
        if (openAiModel != null) {
            log.info("Configuring production resilient Spring AI EmbeddingModel with automatic deterministic fallback");
            return new EmbeddingModel() {
                @Override
                public EmbeddingResponse call(EmbeddingRequest request) {
                    try {
                        return openAiModel.call(request);
                    } catch (Exception e) {
                        log.warn("OpenAI Embedding call failed ({}). Delegating to deterministic fallback.", e.getMessage());
                        return fallbackModel.call(request);
                    }
                }

                @Override
                public float[] embed(Document document) {
                    try {
                        return openAiModel.embed(document);
                    } catch (Exception e) {
                        log.warn("OpenAI Document embedding failed ({}). Delegating to deterministic fallback.", e.getMessage());
                        return fallbackModel.embed(document);
                    }
                }

                @Override
                public float[] embed(String text) {
                    try {
                        return openAiModel.embed(text);
                    } catch (Exception e) {
                        log.warn("OpenAI Text embedding failed ({}). Delegating to deterministic fallback.", e.getMessage());
                        return fallbackModel.embed(text);
                    }
                }

                @Override
                public int dimensions() {
                    return EMBEDDING_DIMENSION;
                }
            };
        }

        log.info("Configuring 1536-dimensional deterministic vector embedding model as missing bean fallback");
        return fallbackModel;
    }

    private static ChatClient createFallbackChatClient() {
        ChatModel mockModel = new ChatModel() {
            @Override
            public ChatResponse call(Prompt prompt) {
                String promptText = prompt.getContents().toLowerCase();
                FailureCategory category;
                boolean safeToReplay;
                int confidence;
                String analysis;
                String remediation;

                if (promptText.contains("timeout") || promptText.contains("connection refused") || promptText.contains("503")) {
                    category = FailureCategory.EPHEMERAL_NETWORK;
                    safeToReplay = true;
                    confidence = 94;
                    analysis = "Downstream target endpoint suffered transient network disconnection or gateway timeout.";
                    remediation = "Permit downstream service cooling interval, then trigger automated retry.";
                } else if (promptText.contains("401") || promptText.contains("403") || promptText.contains("unauthorized")) {
                    category = FailureCategory.AUTH_FAILURE;
                    safeToReplay = false;
                    confidence = 98;
                    analysis = "Downstream endpoint rejected webhook due to expired or invalid subscriber credentials.";
                    remediation = "Rotate tenant subscriber token before attempting manual replay.";
                } else if (promptText.contains("json") || promptText.contains("deserialize") || promptText.contains("schema") || promptText.contains("422")) {
                    category = FailureCategory.SCHEMA_MISMATCH;
                    safeToReplay = false;
                    confidence = 92;
                    analysis = "Inbound webhook payload structure was rejected by subscriber contract schema validator.";
                    remediation = "Review subscriber OpenAPI contract and patch schema transformer.";
                } else {
                    category = FailureCategory.INTERNAL_SERVER_ERROR;
                    safeToReplay = true;
                    confidence = 88;
                    analysis = "Downstream server returned HTTP 500 Internal Server Error.";
                    remediation = "Inspect downstream service application logs and re-dispatch once healthy.";
                }

                String jsonResponse = String.format("""
                    {
                      "failureCategory": "%s",
                      "confidenceScore": %d,
                      "isSafeToAutoReplay": %s,
                      "rootCauseAnalysis": "%s",
                      "recommendedRemediation": "%s"
                    }
                    """, category.name(), confidence, safeToReplay, analysis, remediation);

                return new ChatResponse(List.of(new Generation(new org.springframework.ai.chat.messages.AssistantMessage(jsonResponse))));
            }
        };

        return ChatClient.builder(mockModel).build();
    }

    private static EmbeddingModel createFallbackEmbeddingModel() {
        return new EmbeddingModel() {
            @Override
            public EmbeddingResponse call(EmbeddingRequest request) {
                List<Embedding> embeddings = new ArrayList<>();
                List<String> instructions = request.getInstructions();
                for (int i = 0; i < instructions.size(); i++) {
                    embeddings.add(new Embedding(embed(instructions.get(i)), i));
                }
                return new EmbeddingResponse(embeddings);
            }

            @Override
            public float[] embed(Document document) {
                return embed(document.getText());
            }

            @Override
            public float[] embed(String text) {
                return generateDeterministicVector(text, EMBEDDING_DIMENSION);
            }

            @Override
            public int dimensions() {
                return EMBEDDING_DIMENSION;
            }
        };
    }

    /**
     * Generates a normalized 1536-dimensional unit vector deterministically from text
     * using SHA-256 seed hashing.
     */
    public static float[] generateDeterministicVector(String text, int dimensions) {
        float[] vector = new float[dimensions];
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest((text != null ? text : "").getBytes(StandardCharsets.UTF_8));
            double sumSquares = 0.0;

            for (int i = 0; i < dimensions; i++) {
                int byteIndex = (i * 7) % hash.length;
                float val = ((hash[byteIndex] & 0xFF) - 128) / 128.0f;
                vector[i] = val;
                sumSquares += val * val;
            }

            double norm = Math.sqrt(sumSquares);
            if (norm > 0) {
                for (int i = 0; i < dimensions; i++) {
                    vector[i] = (float) (vector[i] / norm);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm missing", e);
        }
        return vector;
    }
}
