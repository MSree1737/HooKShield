package com.hookshield.domain.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dlq_incident_reports")
public class DlqIncidentReport {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "delivery_id", nullable = false)
    private WebhookDelivery delivery;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id")
    private ErrorCluster cluster;

    @Column(name = "http_status_code")
    private Integer httpStatusCode;

    @Column(name = "raw_error_message", columnDefinition = "TEXT")
    private String rawErrorMessage;

    @Column(name = "ai_root_cause_analysis", columnDefinition = "TEXT")
    private String aiRootCauseAnalysis;

    @Column(name = "ai_confidence_score")
    private Integer aiConfidenceScore;

    @Column(nullable = false)
    private boolean resolved = false;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    public DlqIncidentReport() {}

    public DlqIncidentReport(WebhookDelivery delivery, ErrorCluster cluster, Integer httpStatusCode,
                              String rawErrorMessage, String aiRootCauseAnalysis, Integer aiConfidenceScore) {
        this.delivery = delivery;
        this.cluster = cluster;
        this.httpStatusCode = httpStatusCode;
        this.rawErrorMessage = rawErrorMessage;
        this.aiRootCauseAnalysis = aiRootCauseAnalysis;
        this.aiConfidenceScore = aiConfidenceScore;
        this.resolved = false;
        this.createdAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public WebhookDelivery getDelivery() {
        return delivery;
    }

    public void setDelivery(WebhookDelivery delivery) {
        this.delivery = delivery;
    }

    public ErrorCluster getCluster() {
        return cluster;
    }

    public void setCluster(ErrorCluster cluster) {
        this.cluster = cluster;
    }

    public Integer getHttpStatusCode() {
        return httpStatusCode;
    }

    public void setHttpStatusCode(Integer httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public String getRawErrorMessage() {
        return rawErrorMessage;
    }

    public void setRawErrorMessage(String rawErrorMessage) {
        this.rawErrorMessage = rawErrorMessage;
    }

    public String getAiRootCauseAnalysis() {
        return aiRootCauseAnalysis;
    }

    public void setAiRootCauseAnalysis(String aiRootCauseAnalysis) {
        this.aiRootCauseAnalysis = aiRootCauseAnalysis;
    }

    public Integer getAiConfidenceScore() {
        return aiConfidenceScore;
    }

    public void setAiConfidenceScore(Integer aiConfidenceScore) {
        this.aiConfidenceScore = aiConfidenceScore;
    }

    public boolean isResolved() {
        return resolved;
    }

    public void setResolved(boolean resolved) {
        this.resolved = resolved;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}
