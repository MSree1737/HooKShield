package com.hookshield.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hookshield.domain.entity.DlqIncidentReport;
import com.hookshield.domain.entity.ErrorCluster;
import com.hookshield.domain.entity.Tenant;
import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.FailureCategory;
import com.hookshield.domain.record.ReplayRequest;
import com.hookshield.domain.record.ReplayResponse;
import com.hookshield.repository.DlqIncidentReportRepository;
import com.hookshield.repository.ErrorClusterRepository;
import com.hookshield.service.DlqReplayService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DlqManagementController.class)
class DlqManagementControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ErrorClusterRepository clusterRepository;

    @MockBean
    private DlqIncidentReportRepository incidentReportRepository;

    @MockBean
    private DlqReplayService replayService;

    @Test
    @DisplayName("Should return list of error clusters with summary statistics")
    void testListClusters() throws Exception {
        ErrorCluster cluster = new ErrorCluster(
            "Cluster 1", FailureCategory.EPHEMERAL_NETWORK, "[0.1,0.2]", "Retry later", true
        );
        cluster.setId(UUID.randomUUID());
        cluster.setTotalOccurrences(15);

        when(clusterRepository.findAll(any(org.springframework.data.domain.Sort.class)))
            .thenReturn(List.of(cluster));

        mockMvc.perform(get("/api/v1/dlq/clusters"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].clusterName").value("Cluster 1"))
            .andExpect(jsonPath("$[0].failureCategory").value("EPHEMERAL_NETWORK"))
            .andExpect(jsonPath("$[0].totalOccurrences").value(15))
            .andExpect(jsonPath("$[0].isSafeToAutoReplay").value(true));
    }

    @Test
    @DisplayName("Should return paginated incidents for a given cluster")
    void testListIncidents() throws Exception {
        UUID clusterId = UUID.randomUUID();
        UUID deliveryId = UUID.randomUUID();

        Tenant tenant = new Tenant(UUID.randomUUID(), "Tenant A", 500, 1000);
        WebhookDelivery delivery = new WebhookDelivery(tenant, "STRIPE", "evt_1", "hash", "{}", "https://dest.com");
        delivery.setId(deliveryId);

        ErrorCluster cluster = new ErrorCluster("Cluster 1", FailureCategory.EPHEMERAL_NETWORK, "[]", "action", true);
        cluster.setId(clusterId);

        DlqIncidentReport report = new DlqIncidentReport(
            delivery, cluster, 503, "Service Unavailable", "Network timeout", 90
        );
        report.setId(UUID.randomUUID());

        when(incidentReportRepository.findByClusterId(eq(clusterId), any(Pageable.class)))
            .thenReturn(new PageImpl<>(List.of(report)));

        mockMvc.perform(get("/api/v1/dlq/incidents").param("clusterId", clusterId.toString()))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.content[0].httpStatusCode").value(503))
            .andExpect(jsonPath("$.content[0].deliveryId").value(deliveryId.toString()));
    }

    @Test
    @DisplayName("Should execute bulk replay and return replayed count")
    void testReplayWebhooks() throws Exception {
        UUID deliveryId = UUID.randomUUID();
        ReplayRequest request = new ReplayRequest(null, null, List.of(deliveryId), null, null);
        ReplayResponse response = new ReplayResponse(1, "Successfully triggered replay for 1 webhooks", List.of(deliveryId));

        when(replayService.replayDeliveries(null, null, List.of(deliveryId))).thenReturn(response);

        mockMvc.perform(post("/api/v1/dlq/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.replayedCount").value(1))
            .andExpect(jsonPath("$.replayedIds[0]").value(deliveryId.toString()));
    }
}
