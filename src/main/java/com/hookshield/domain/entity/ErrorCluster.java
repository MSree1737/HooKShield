package com.hookshield.domain.entity;

import com.hookshield.domain.enums.FailureCategory;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "error_clusters")
public class ErrorCluster {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "cluster_name", nullable = false, length = 200)
    private String clusterName;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_category", nullable = false, length = 50)
    private FailureCategory failureCategory;

    /**
     * Stored in PostgreSQL as vector(1536).
     * Represented as a string formatted as "[x1, x2, ...]" for JDBC and pgvector compatibility.
     */
    @org.hibernate.annotations.ColumnTransformer(write = "?::vector")
    @Column(name = "signature_embedding", nullable = false, columnDefinition = "vector(1536)")
    private String signatureEmbedding;

    @Column(name = "recommended_action", columnDefinition = "TEXT")
    private String recommendedAction;

    @Column(name = "is_safe_to_autoreplay", nullable = false)
    private boolean isSafeToAutoReplay = false;

    @Column(name = "total_occurrences", nullable = false)
    private int totalOccurrences = 1;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public ErrorCluster() {}

    public ErrorCluster(String clusterName, FailureCategory failureCategory, String signatureEmbedding,
                        String recommendedAction, boolean isSafeToAutoReplay) {
        this.clusterName = clusterName;
        this.failureCategory = failureCategory;
        this.signatureEmbedding = signatureEmbedding;
        this.recommendedAction = recommendedAction;
        this.isSafeToAutoReplay = isSafeToAutoReplay;
        this.totalOccurrences = 1;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = Instant.now();
    }

    public void incrementOccurrences() {
        this.totalOccurrences++;
        this.updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public FailureCategory getFailureCategory() {
        return failureCategory;
    }

    public void setFailureCategory(FailureCategory failureCategory) {
        this.failureCategory = failureCategory;
    }

    public String getSignatureEmbedding() {
        return signatureEmbedding;
    }

    public void setSignatureEmbedding(String signatureEmbedding) {
        this.signatureEmbedding = signatureEmbedding;
    }

    public String getRecommendedAction() {
        return recommendedAction;
    }

    public void setRecommendedAction(String recommendedAction) {
        this.recommendedAction = recommendedAction;
    }

    public boolean isSafeToAutoReplay() {
        return isSafeToAutoReplay;
    }

    public void setSafeToAutoReplay(boolean safeToAutoReplay) {
        isSafeToAutoReplay = safeToAutoReplay;
    }

    public int getTotalOccurrences() {
        return totalOccurrences;
    }

    public void setTotalOccurrences(int totalOccurrences) {
        this.totalOccurrences = totalOccurrences;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
