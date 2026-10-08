package com.hookshield.repository;

import com.hookshield.domain.entity.ErrorCluster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ErrorClusterRepository extends JpaRepository<ErrorCluster, UUID> {

    /**
     * Executes native pgvector cosine distance query using the <=> operator.
     * Distance <= maxDistance corresponds to cosine similarity >= (1.0 - maxDistance).
     * For example, maxDistance = 0.12 corresponds to >= 88% similarity.
     */
    @Query(value = """
        SELECT * FROM error_clusters
        WHERE (signature_embedding <=> CAST(:embedding AS vector)) <= :maxDistance
        ORDER BY signature_embedding <=> CAST(:embedding AS vector) ASC
        LIMIT 1
        """, nativeQuery = true)
    Optional<ErrorCluster> findClosestClusterWithinDistance(
        @Param("embedding") String embedding,
        @Param("maxDistance") double maxDistance
    );

    @Query(value = """
        SELECT * FROM error_clusters
        ORDER BY signature_embedding <=> CAST(:embedding AS vector) ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<ErrorCluster> findNearestClusters(
        @Param("embedding") String embedding,
        @Param("limit") int limit
    );
}
