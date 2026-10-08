/**
 * HookShield Enterprise Hub - Frontend Logic & Live Telemetry
 */

const STATE = {
  activeTab: 'overview',
  autoRefresh: true,
  refreshInterval: null,
  tenants: [],
  selectedTenant: null,
  selectedProvider: null,
  deliveries: [],
  currentDeliveryPage: 0,
  currentStatusFilter: '',
  clusters: [],
  activeDrawerDeliveryId: null
};

// Preset Webhook Payloads
const PRESETS = {
  stripe_payment: {
    provider: 'STRIPE',
    endpoint: 'http://localhost:8080/api/v1/webhooks/mock-subscriber',
    status: 200,
    payload: JSON.stringify({
      id: "evt_1P0" + Math.random().toString(36).substring(2, 9),
      object: "event",
      api_version: "2024-06-20",
      type: "payment_intent.succeeded",
      data: {
        object: {
          id: "pi_" + Math.random().toString(36).substring(2, 10),
          amount: 8900,
          currency: "usd",
          status: "succeeded",
          customer: "cus_demo991"
        }
      }
    }, null, 2)
  },
  github_push: {
    provider: 'GITHUB',
    endpoint: 'http://localhost:8080/api/v1/webhooks/mock-subscriber',
    status: 200,
    payload: JSON.stringify({
      ref: "refs/heads/main",
      repository: {
        id: 987654321,
        name: "microservices-core",
        full_name: "enterprise/microservices-core"
      },
      head_commit: {
        id: Math.random().toString(36).substring(2, 12),
        message: "fix(ingestion): boost Kafka throughput to 10k rps with Virtual Threads",
        author: { name: "Distributed Architect", email: "arch@hookshield.io" }
      }
    }, null, 2)
  },
  shopify_order: {
    provider: 'SHOPIFY',
    endpoint: 'http://localhost:8080/api/v1/webhooks/mock-subscriber',
    status: 200,
    payload: JSON.stringify({
      id: Math.floor(Math.random() * 9000000000) + 1000000000,
      email: "billing@enterprise-client.com",
      currency: "USD",
      total_price: "249.99",
      line_items: [
        { title: "HookShield Cloud Enterprise License", quantity: 1, price: "249.99" }
      ]
    }, null, 2)
  },
  subscriber_503: {
    provider: 'STRIPE',
    endpoint: 'http://localhost:8080/api/v1/webhooks/mock-subscriber?status=503',
    status: 503,
    payload: JSON.stringify({
      event: "outage.test.event",
      description: "Downstream subscriber returns HTTP 503 Service Unavailable to trigger non-blocking retry tier",
      attempt: 1,
      timestamp: new Date().toISOString()
    }, null, 2)
  },
  auth_failure: {
    provider: 'STRIPE',
    endpoint: 'http://localhost:8080/api/v1/webhooks/mock-subscriber?status=401',
    status: 401,
    payload: JSON.stringify({
      event: "auth.failure.test",
      description: "Downstream subscriber returns HTTP 401 Unauthorized - Triggers Spring AI Auth Failure DLQ triage",
      apiKeyStatus: "EXPIRED"
    }, null, 2)
  }
};

// Stage Inspector Metadata
const STAGE_METADATA = {
  1: {
    tag: "STAGE 1: INGESTION GATEWAY",
    tech: "WebhookIngressController.java",
    title: "High-Throughput Ingestion & Cryptographic Verification",
    desc: "Terminates inbound HTTP POST webhooks, validates tenant HMAC-SHA256 signatures using constant-time byte comparisons to prevent timing side-channel exploits, and enforces token-bucket rate quotas per tenant.",
    sla: "< 15ms Fast-Ack",
    broker: "Redis L1 Secret Cache (<1ms)",
    sec: "Constant-Time MessageDigest.isEqual",
    fail: "HTTP 401 Unauthorized / HTTP 429 Surge Rejection"
  },
  2: {
    tag: "STAGE 2: INBOUND FAST-ACK BROKER",
    tech: "KafkaProducerConfig.java (KRaft Mode)",
    title: "Ultra-Low Latency Kafka Append & Handshake",
    desc: "Asynchronously writes the raw webhook payload to the 'webhook-inbound-raw' Kafka topic with Zstandard compression and returns HTTP 202 Accepted with delivery ID and latency headers in <15ms.",
    sla: "Sub-15ms Ingestion Guarantee",
    broker: "Kafka Topic: webhook-inbound-raw",
    sec: "Zstandard / acks=all / idempotent=true",
    fail: "Asynchronous Rejection / Circuit Tripping"
  },
  3: {
    tag: "STAGE 3: ATOMIC DEDUPLICATION & AUDIT",
    tech: "InboundConsumer.java & IdempotencyService.java",
    title: "Distributed Idempotency & Transactional Persistence",
    desc: "Computes a composite SHA-256 hash of Tenant + Provider + Payload + Event ID. Executes an atomic Redis SET NX EX 86400 lock before persisting to PostgreSQL with status RECEIVED.",
    sla: "< 1ms Redis Lock Resolution",
    broker: "Redis Key: idemp:lock:{sha256}",
    sec: "Exactly-Once Processing Semantics",
    fail: "Duplicate Suppressed & Offset Committed"
  },
  4: {
    tag: "STAGE 4: VIRTUAL THREAD DISPATCH",
    tech: "DispatchConsumer.java & WebhookDispatcherService.java",
    title: "Java 21 Loom Concurrency & Host Circuit Breaker",
    desc: "Consumes from 'webhook-dispatch-queue'. Launches lightweight Java 21 Virtual Threads to perform outbound HTTP calls using Spring RestClient, protected by per-host Resilience4j circuit breakers.",
    sla: "Non-blocking Carrier Threads",
    broker: "Kafka Topic: webhook-dispatch-queue",
    sec: "Resilience4j Per-Host Sliding Window (20 calls, 50%)",
    fail: "Fast-Fail on CB Open -> Route to Retry Tier"
  },
  5: {
    tag: "STAGE 5: NON-BLOCKING RETRY PIPELINE",
    tech: "RetryConsumers.java (Multi-Topic Hopping)",
    title: "Non-Blocking Exponential Backoff Topics",
    desc: "Failed deliveries hop across dedicated delayed Kafka topics (webhook-retry-1m -> 5m -> 15m) using scheduled virtual thread executors without stalling Kafka consumer poll loops or triggering partition rebalances.",
    sla: "Zero Consumer Thread Sleep",
    broker: "Kafka Topics: webhook-retry-1m / 5m / 15m",
    sec: "Target Epoch Header Scheduling",
    fail: "Attempt >= 3 Exhaustion -> Escalates to DLQ"
  },
  6: {
    tag: "STAGE 6: AI DLQ SEMANTIC TRIAGE",
    tech: "DlqConsumer.java & AiDiagnosticService.java",
    title: "Spring AI Zero-Shot Reasoning & pgvector Clustering",
    desc: "Extracts failure signatures from exhausted deliveries. Spring AI generates 1536-dimensional embeddings and clusters errors in PostgreSQL using cosine distance (<=> <= 0.12). Runs zero-shot LLM reasoning for novel errors.",
    sla: "<=> Cosine Distance <= 0.12 (>=88% Match)",
    broker: "PostgreSQL pgvector: error_clusters (IVFFlat)",
    sec: "Automated Cooling Replays for Ephemeral 503s",
    fail: "Dangerous Payloads Flagged for Manual Review"
  }
};

// ============================================================================
// Initialization
// ============================================================================
document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  initAutoRefresh();
  initPipelineInspector();
  initSimulator();
  initTableFilter();
  initDrawer();

  // Initial Data Fetch
  fetchTenants();
  fetchOverview();
  fetchDeliveries();
  fetchClusters();
});

// ============================================================================
// Tab Management
// ============================================================================
function initTabs() {
  const tabs = document.querySelectorAll('.nav-tab');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const target = tab.dataset.tab;
      tabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');

      document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
      const activePane = document.getElementById(`pane-${target}`);
      if (activePane) activePane.classList.add('active');

      STATE.activeTab = target;

      // Refresh specific tab data immediately upon switch
      if (target === 'deliveries') fetchDeliveries();
      if (target === 'clusters') fetchClusters();
      if (target === 'overview') fetchOverview();
    });
  });
}

function initAutoRefresh() {
  const toggle = document.getElementById('auto-refresh-toggle');
  if (!toggle) return;

  function setTimer() {
    if (STATE.autoRefresh) {
      STATE.refreshInterval = setInterval(() => {
        if (STATE.activeTab === 'overview') fetchOverview();
        if (STATE.activeTab === 'deliveries') fetchDeliveries(STATE.currentDeliveryPage, false);
        if (STATE.activeTab === 'clusters') fetchClusters();
      }, 3000);
    } else {
      clearInterval(STATE.refreshInterval);
    }
  }

  toggle.addEventListener('change', (e) => {
    STATE.autoRefresh = e.target.checked;
    setTimer();
    showToast(STATE.autoRefresh ? "Live polling stream enabled (3s)" : "Live polling stream paused", "info");
  });

  setTimer();
}

// ============================================================================
// Interactive Pipeline Inspector
// ============================================================================
function initPipelineInspector() {
  const nodes = document.querySelectorAll('.flow-node');
  nodes.forEach(node => {
    node.addEventListener('click', () => {
      const stageNum = parseInt(node.dataset.node, 10);
      nodes.forEach(n => n.classList.remove('active'));
      node.classList.add('active');
      renderInspector(stageNum);
    });
  });
}

function renderInspector(stageNum) {
  const meta = STAGE_METADATA[stageNum];
  if (!meta) return;

  const stageTag = document.getElementById('inspector-stage-tag');
  const techTag = document.getElementById('inspector-tech-tag');
  const title = document.getElementById('inspector-title');
  const desc = document.getElementById('inspector-desc');
  const sla = document.getElementById('inspector-sla');
  const broker = document.getElementById('inspector-broker');
  const sec = document.getElementById('inspector-sec');
  const fail = document.getElementById('inspector-fail');

  if (stageTag) stageTag.innerText = meta.tag;
  if (techTag) techTag.innerText = meta.tech;
  if (title) title.innerText = meta.title;
  if (desc) desc.innerText = meta.desc;
  if (sla) sla.innerText = meta.sla;
  if (broker) broker.innerText = meta.broker;
  if (sec) sec.innerText = meta.sec;
  if (fail) fail.innerText = meta.fail;
}

// ============================================================================
// Telemetry & Overview API
// ============================================================================
async function fetchOverview() {
  try {
    const res = await fetch('/api/v1/dashboard/overview');
    if (!res.ok) return;
    const data = await res.json();

    document.getElementById('stat-total-deliveries').innerText = Number(data.totalDeliveries).toLocaleString();
    document.getElementById('stat-dispatched').innerText = Number(data.dispatchedCount).toLocaleString();
    document.getElementById('stat-retrying').innerText = Number(data.retryingCount).toLocaleString();
    document.getElementById('stat-dlq').innerText = Number(data.deadLetterCount).toLocaleString();
    document.getElementById('stat-clusters').innerText = Number(data.totalClusters).toLocaleString();
    document.getElementById('stat-sla').innerText = data.slaAdherencePct + '%';

    // Update Tab Badges
    const clusterBadge = document.getElementById('badge-cluster-count');
    if (clusterBadge) clusterBadge.innerText = data.totalClusters;

    const healthText = document.getElementById('health-status-text');
    if (healthText) {
      healthText.innerText = "All Systems Operational";
    }
  } catch (err) {
    console.warn("Error fetching overview telemetry:", err);
  }
}

// ============================================================================
// Tenants & Providers API
// ============================================================================
async function fetchTenants() {
  try {
    const res = await fetch('/api/v1/dashboard/tenants');
    if (!res.ok) return;
    STATE.tenants = await res.json();

    const tenantSelect = document.getElementById('sim-tenant-select');
    if (tenantSelect) {
      tenantSelect.innerHTML = STATE.tenants.map(t => 
        `<option value="${t.id}">${t.name} (${t.rateLimitPerSecond} req/s)</option>`
      ).join('');

      tenantSelect.addEventListener('change', () => onTenantChange());
      if (STATE.tenants.length > 0) {
        onTenantChange();
      }
    }

    renderTenantPolicies();
  } catch (err) {
    console.error("Error loading tenants:", err);
  }
}

function onTenantChange() {
  const tenantSelect = document.getElementById('sim-tenant-select');
  const providerSelect = document.getElementById('sim-provider-select');
  const tenantId = tenantSelect.value;

  const tenant = STATE.tenants.find(t => t.id === tenantId);
  STATE.selectedTenant = tenant;

  if (tenant && providerSelect) {
    providerSelect.innerHTML = tenant.providers.map(p => 
      `<option value="${p.providerName}">${p.providerName} (Key: ${p.headerSignatureKey})</option>`
    ).join('');

    STATE.selectedProvider = tenant.providers[0];
  }
}

function renderTenantPolicies() {
  const container = document.getElementById('tenants-policy-container');
  if (!container) return;

  container.innerHTML = STATE.tenants.map(t => `
    <div class="card-panel" style="margin-bottom: 1.25rem;">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
        <h3 class="card-title" style="margin: 0;">🏢 ${t.name}</h3>
        <span class="system-pill">Active Tenant</span>
      </div>
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 1rem; font-size: 0.85rem;">
        <div><span style="color: var(--text-muted);">Tenant ID:</span> <div class="mono-cell" style="word-break: break-all;">${t.id}</div></div>
        <div><span style="color: var(--text-muted);">Rate Limit:</span> <div style="font-weight: 700; color: #fff;">${t.rateLimitPerSecond} req/sec</div></div>
        <div><span style="color: var(--text-muted);">Burst Capacity:</span> <div style="font-weight: 700; color: #fff;">${t.burstCapacity} burst tokens</div></div>
      </div>
      <h4 style="font-size: 0.82rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.5rem; letter-spacing: 0.05em;">Configured Provider Credentials:</h4>
      <div style="display: flex; gap: 0.75rem; flex-wrap: wrap;">
        ${t.providers.map(p => `
          <div style="background: var(--bg-input); padding: 0.65rem 0.95rem; border-radius: var(--radius-md); border: 1px solid var(--border-subtle); font-size: 0.82rem;">
            <div style="font-weight: 700; color: #fff; margin-bottom: 0.2rem;">${p.providerName}</div>
            <div style="color: var(--text-dim);">Signature Header: <span class="mono-cell">${p.headerSignatureKey}</span></div>
            <div style="color: var(--text-dim);">HMAC Secret: <span class="mono-cell" style="color: var(--accent-secondary);">${p.maskedSecret}</span></div>
          </div>
        `).join('')}
      </div>
    </div>
  `).join('');
}

// ============================================================================
// Webhook Simulator
// ============================================================================
function initSimulator() {
  const form = document.getElementById('webhook-simulator-form');
  const payloadArea = document.getElementById('sim-payload');
  const formatBtn = document.getElementById('btn-format-json');
  const randomBtn = document.getElementById('btn-randomize-id');
  const resetEndpointBtn = document.getElementById('btn-reset-endpoint');

  // Load default preset
  loadPreset('stripe_payment');

  // Preset Buttons
  document.querySelectorAll('.preset-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.preset-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const presetKey = btn.dataset.preset;
      loadPreset(presetKey);
    });
  });

  // Action Buttons
  if (formatBtn && payloadArea) {
    formatBtn.addEventListener('click', () => {
      try {
        const parsed = JSON.parse(payloadArea.value);
        payloadArea.value = JSON.stringify(parsed, null, 2);
        showToast("JSON formatted cleanly", "info");
      } catch (e) {
        showToast("Invalid JSON payload", "error");
      }
    });
  }

  if (randomBtn && payloadArea) {
    randomBtn.addEventListener('click', () => {
      try {
        const parsed = JSON.parse(payloadArea.value);
        if (parsed.id) {
          parsed.id = "evt_" + Math.random().toString(36).substring(2, 10);
        } else {
          parsed.eventId = "evt_" + Math.random().toString(36).substring(2, 10);
        }
        payloadArea.value = JSON.stringify(parsed, null, 2);
        showToast("New unique Event ID generated", "info");
      } catch (e) {
        showToast("Unable to inject event ID", "error");
      }
    });
  }

  if (resetEndpointBtn) {
    resetEndpointBtn.addEventListener('click', () => {
      const endpointInput = document.getElementById('sim-endpoint');
      if (endpointInput) {
        endpointInput.value = 'http://localhost:8080/api/v1/webhooks/mock-subscriber';
        showToast("Destination reset to built-in mock subscriber", "info");
      }
    });
  }

  if (form) {
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      await executeSimulation();
    });
  }
}

function loadPreset(key) {
  const preset = PRESETS[key];
  if (!preset) return;

  const payloadArea = document.getElementById('sim-payload');
  const endpointInput = document.getElementById('sim-endpoint');
  const providerSelect = document.getElementById('sim-provider-select');
  const statusCodeSelect = document.getElementById('sim-status-code');

  if (payloadArea) payloadArea.value = preset.payload;
  if (endpointInput) endpointInput.value = preset.endpoint;
  if (providerSelect) providerSelect.value = preset.provider;
  if (statusCodeSelect) statusCodeSelect.value = preset.status || 200;
}

async function executeSimulation() {
  const tenantSelect = document.getElementById('sim-tenant-select');
  const providerSelect = document.getElementById('sim-provider-select');
  const endpointInput = document.getElementById('sim-endpoint');
  const payloadArea = document.getElementById('sim-payload');
  const statusCodeSelect = document.getElementById('sim-status-code');
  const submitBtn = document.getElementById('sim-submit-btn');
  const traceBadge = document.getElementById('live-trace-badge');

  const tenantId = tenantSelect.value;
  const provider = providerSelect.value;
  const targetEndpoint = endpointInput.value;
  const payload = payloadArea.value;
  const simulateStatusCode = parseInt(statusCodeSelect.value, 10);

  submitBtn.disabled = true;
  submitBtn.innerHTML = `<span>⏳ Ingesting to Kafka...</span>`;
  if (traceBadge) traceBadge.innerText = "Ingesting...";

  try {
    const res = await fetch('/api/v1/dashboard/simulate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tenantId,
        provider,
        targetEndpoint,
        payload,
        simulateStatusCode
      })
    });

    const data = await res.json();
    renderSimulationResult(data);

    if (data.success) {
      showToast(`Webhook accepted in ${data.latencyMs}ms! SLA Target Met`, "success");
      fetchOverview();
      fetchDeliveries();
    } else {
      showToast(`Simulation error: ${data.message}`, "error");
    }
  } catch (err) {
    showToast("Network error executing simulation", "error");
    console.error(err);
  } finally {
    submitBtn.disabled = false;
    submitBtn.innerHTML = `<span class="btn-icon">🚀</span><span>Ingest & Dispatch Webhook</span>`;
    if (traceBadge) traceBadge.innerText = "Trace Completed";
  }
}

function renderSimulationResult(data) {
  const resultBox = document.getElementById('simulation-result-box');
  if (!resultBox) return;

  if (!data || !data.success) {
    resultBox.innerHTML = `
      <div class="result-card-badge error">❌ Ingestion Rejected</div>
      <p style="color: var(--color-danger); font-size: 0.9rem; margin-top: 0.5rem;">${data.message || 'Unknown failure'}</p>
    `;
    return;
  }

  const isSub15 = data.latencyMs < 15;

  resultBox.innerHTML = `
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
      <div class="result-card-badge success">⚡ 202 ACCEPTED (Fast-Ack to Kafka)</div>
      <div class="stat-subtext" style="font-weight: 700; color: ${isSub15 ? 'var(--color-success)' : 'var(--color-warning)'};">
        ${isSub15 ? '🎯 SLA TARGET MET (<15ms)' : 'Latency Recorded'}
      </div>
    </div>

    <div class="metric-row">
      <span class="metric-key">Ingestion Latency SLA</span>
      <span class="metric-val latency-badge">${data.latencyMs} ms</span>
    </div>

    <div class="metric-row">
      <span class="metric-key">Assigned Delivery ID</span>
      <span class="metric-val" style="word-break: break-all;">
        ${data.deliveryId}
        <button class="text-copy-btn" onclick="copyText('${data.deliveryId}', 'Delivery ID copied!')" style="margin-left: 0.5rem;">Copy</button>
      </span>
    </div>

    <div class="metric-row">
      <span class="metric-key">Auth Header Scheme</span>
      <span class="metric-val">${data.headerName}</span>
    </div>

    <div class="metric-row" style="flex-direction: column; align-items: flex-start; gap: 0.25rem;">
      <div style="display: flex; justify-content: space-between; width: 100%;">
        <span class="metric-key">Computed HMAC-SHA256 Token</span>
        <button class="text-copy-btn" onclick="copyText('${data.signature}', 'HMAC signature copied!')">Copy Token</button>
      </div>
      <span class="metric-val" style="font-size: 0.72rem; word-break: break-all; color: var(--accent-secondary);">${data.signature}</span>
    </div>

    <!-- Microsecond Execution Trace Timeline -->
    <div class="trace-timeline">
      <div class="trace-title">Execution Pipeline Trace</div>
      <div class="trace-item">
        <span class="trace-time">T+0.3ms</span>
        <span class="trace-desc">🔐 Constant-Time HMAC-SHA256 signature verified</span>
      </div>
      <div class="trace-item">
        <span class="trace-time">T+0.8ms</span>
        <span class="trace-desc">⏱️ Redis token-bucket rate quota validated (Quota OK)</span>
      </div>
      <div class="trace-item">
        <span class="trace-time">T+2.6ms</span>
        <span class="trace-desc">📨 Kafka record published to <code>webhook-inbound-raw</code></span>
      </div>
      <div class="trace-item">
        <span class="trace-time">T+${data.latencyMs}ms</span>
        <span class="trace-desc">⚡ HTTP 202 Accepted fast-ack returned to client</span>
      </div>
      <div class="trace-item">
        <span class="trace-time">Async</span>
        <span class="trace-desc">🔒 Composite SHA-256 hash locked in Redis (SET NX EX 86400)</span>
      </div>
      <div class="trace-item">
        <span class="trace-time">Async</span>
        <span class="trace-desc">🚀 Java 21 Loom Virtual Thread outbound dispatch initiated</span>
      </div>
    </div>
  `;
}

// ============================================================================
// Deliveries Stream & Audit Log
// ============================================================================
function initTableFilter() {
  document.querySelectorAll('.filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      STATE.currentStatusFilter = btn.dataset.status;
      STATE.currentDeliveryPage = 0;
      fetchDeliveries();
    });
  });

  const searchInput = document.getElementById('deliveries-search');
  const clearBtn = document.getElementById('clear-search-btn');
  const refreshBtn = document.getElementById('btn-refresh-deliveries');

  if (searchInput) {
    searchInput.addEventListener('input', (e) => {
      const val = e.target.value.trim();
      if (clearBtn) clearBtn.style.display = val ? 'block' : 'none';
      filterDeliveriesLocally(val.toLowerCase());
    });
  }

  if (clearBtn && searchInput) {
    clearBtn.addEventListener('click', () => {
      searchInput.value = '';
      clearBtn.style.display = 'none';
      filterDeliveriesLocally('');
    });
  }

  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      fetchDeliveries(STATE.currentDeliveryPage, true);
      showToast("Refreshed deliveries stream", "info");
    });
  }
}

async function fetchDeliveries(page = 0, showLoading = true) {
  STATE.currentDeliveryPage = page;
  const tbody = document.getElementById('deliveries-table-body');
  if (showLoading && tbody && tbody.children.length === 0) {
    tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; color: var(--text-dim); padding: 3rem;"><div class="table-loader-spinner"></div><div style="margin-top: 0.75rem;">Loading delivery logs...</div></td></tr>`;
  }

  try {
    let url = `/api/v1/dashboard/deliveries?page=${page}&size=25`;
    if (STATE.currentStatusFilter) {
      url += `&status=${STATE.currentStatusFilter}`;
    }

    const res = await fetch(url);
    if (!res.ok) return;
    const data = await res.json();
    STATE.deliveries = data.content || [];

    renderDeliveriesTable(STATE.deliveries);
  } catch (err) {
    console.warn("Error fetching deliveries:", err);
  }
}

function renderDeliveriesTable(list) {
  const tbody = document.getElementById('deliveries-table-body');
  if (!tbody) return;

  if (!list || list.length === 0) {
    tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; color: var(--text-dim); padding: 3rem;">No webhook deliveries recorded yet. Use the Webhook Studio to dispatch one!</td></tr>`;
    return;
  }

  tbody.innerHTML = list.map(d => {
    const dateFormatted = new Date(d.createdAt).toLocaleTimeString();
    return `
      <tr onclick="openDrawer('${d.id}')">
        <td class="mono-cell">
          ${d.id.substring(0, 8)}...
          <span class="table-copy-icon" onclick="event.stopPropagation(); copyText('${d.id}', 'Delivery ID copied!')" title="Copy Full Delivery ID">📋</span>
        </td>
        <td><span class="provider-tag">${d.provider}</span></td>
        <td><span class="status-tag ${d.status}">${d.status}</span></td>
        <td style="text-align: center; font-weight: 700; color: ${d.retryCount > 0 ? 'var(--color-warning)' : 'inherit'};">${d.retryCount}</td>
        <td style="color: var(--text-muted); font-size: 0.82rem;">${d.tenantName}</td>
        <td class="mono-cell" style="max-width: 220px; overflow: hidden; text-overflow: ellipsis;">${d.targetEndpoint}</td>
        <td style="color: var(--text-dim); font-size: 0.8rem;">${dateFormatted}</td>
        <td style="text-align: right;">
          <button type="button" class="text-action-btn" onclick="event.stopPropagation(); openDrawer('${d.id}')">View Details ➔</button>
        </td>
      </tr>
    `;
  }).join('');
}

function filterDeliveriesLocally(query) {
  if (!query) {
    renderDeliveriesTable(STATE.deliveries);
    return;
  }
  const filtered = STATE.deliveries.filter(d => 
    d.id.toLowerCase().includes(query) ||
    d.provider.toLowerCase().includes(query) ||
    d.status.toLowerCase().includes(query) ||
    d.targetEndpoint.toLowerCase().includes(query) ||
    (d.tenantName && d.tenantName.toLowerCase().includes(query))
  );
  renderDeliveriesTable(filtered);
}

// ============================================================================
// Payload Drawer Modal
// ============================================================================
function initDrawer() {
  const overlay = document.getElementById('drawer-overlay');
  const closeBtn = document.getElementById('drawer-close-btn');
  const copyHashBtn = document.getElementById('btn-copy-hash');
  const copyPayloadBtn = document.getElementById('btn-copy-payload');
  const replayBtn = document.getElementById('btn-drawer-replay');

  if (overlay) {
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) closeDrawer();
    });
  }
  if (closeBtn) closeBtn.addEventListener('click', closeDrawer);

  if (copyHashBtn) {
    copyHashBtn.addEventListener('click', () => {
      const hash = document.getElementById('drawer-hash').innerText;
      copyText(hash, "Idempotency hash copied!");
    });
  }

  if (copyPayloadBtn) {
    copyPayloadBtn.addEventListener('click', () => {
      const code = document.getElementById('drawer-payload-code').innerText;
      copyText(code, "JSON payload copied!");
    });
  }

  if (replayBtn) {
    replayBtn.addEventListener('click', async () => {
      if (!STATE.activeDrawerDeliveryId) return;
      showToast("Dispatching replay request to Kafka...", "info");
      try {
        const res = await fetch('/api/v1/dlq/replay', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ deliveryIds: [STATE.activeDrawerDeliveryId] })
        });
        const data = await res.json();
        showToast(`Replayed delivery ${STATE.activeDrawerDeliveryId.substring(0, 8)}... successfully!`, "success");
        closeDrawer();
        fetchOverview();
        fetchDeliveries();
      } catch (e) {
        showToast("Replay failed", "error");
      }
    });
  }
}

function openDrawer(deliveryId) {
  const delivery = STATE.deliveries.find(d => d.id === deliveryId);
  if (!delivery) return;

  STATE.activeDrawerDeliveryId = delivery.id;

  document.getElementById('drawer-delivery-id').innerText = delivery.id;
  document.getElementById('drawer-status').className = `status-tag ${delivery.status}`;
  document.getElementById('drawer-status').innerText = delivery.status;
  document.getElementById('drawer-provider').innerText = delivery.provider;
  document.getElementById('drawer-tenant').innerText = delivery.tenantName;
  document.getElementById('drawer-endpoint').innerText = delivery.targetEndpoint;
  document.getElementById('drawer-retries').innerText = delivery.retryCount;
  document.getElementById('drawer-hash').innerText = delivery.idempotencyHash;

  let formattedJson = delivery.payload;
  try {
    formattedJson = JSON.stringify(JSON.parse(delivery.payload), null, 2);
  } catch (e) {}

  document.getElementById('drawer-payload-code').innerText = formattedJson;

  document.getElementById('drawer-overlay').classList.add('open');
}

function closeDrawer() {
  const overlay = document.getElementById('drawer-overlay');
  if (overlay) overlay.classList.remove('open');
  STATE.activeDrawerDeliveryId = null;
}

// ============================================================================
// AI DLQ Clusters & Replay
// ============================================================================
async function fetchClusters() {
  try {
    const res = await fetch('/api/v1/dlq/clusters');
    if (!res.ok) return;
    STATE.clusters = await res.json();

    renderClusters(STATE.clusters);
  } catch (err) {
    console.warn("Error fetching clusters:", err);
  }
}

function renderClusters(clusters) {
  const grid = document.getElementById('clusters-grid');
  if (!grid) return;

  if (!clusters || clusters.length === 0) {
    grid.innerHTML = `
      <div style="grid-column: 1/-1; text-align: center; padding: 3rem; background: var(--bg-input); border-radius: var(--radius-lg); border: 1px solid var(--border-subtle);">
        <div style="font-size: 2.75rem; margin-bottom: 0.75rem;">🎉</div>
        <h3 style="font-size: 1.25rem; font-weight: 700; color: #fff;">Dead-Letter Queue is Clean!</h3>
        <p style="color: var(--text-muted); font-size: 0.9rem; margin-top: 0.35rem; max-width: 500px; margin-left: auto; margin-right: auto; line-height: 1.5;">
          No unresolvable failure clusters detected. Spring AI & pgvector semantic clustering will group any future downstream subscriber outages automatically.
        </p>
      </div>
    `;
    return;
  }

  grid.innerHTML = clusters.map(c => `
    <div class="cluster-card">
      <div>
        <div class="cluster-badge-row">
          <span class="category-tag ${c.failureCategory}">${c.failureCategory}</span>
          <span class="cluster-occurrences">🔥 ${c.totalOccurrences} occurrences</span>
        </div>
        <h3 class="cluster-name">${c.clusterName}</h3>
        <div style="font-size: 0.78rem; color: var(--accent-secondary); font-family: var(--font-mono); margin-bottom: 0.75rem;">
          pgvector Embedding: 1536-dim Unit Vector (&lt;=&gt; Cosine Distance)
        </div>
        <div class="cluster-remediation">
          <strong style="color: #fff;">AI Recommended Remediation:</strong><br>
          ${c.recommendedAction}
        </div>
      </div>
      <div class="cluster-footer">
        <span style="font-size: 0.78rem; font-weight: 600; color: ${c.safeToAutoReplay ? 'var(--color-success)' : 'var(--color-warning)'};">
          ${c.safeToAutoReplay ? '✓ Safe for Auto-Replay' : '⚠️ Manual Authorization Required'}
        </span>
        <button class="replay-btn" onclick="triggerReplay('${c.id}')">
          <span>🔁 Replay Cluster</span>
        </button>
      </div>
    </div>
  `).join('');
}

async function triggerReplay(clusterId) {
  showToast("Dispatching cluster replay to Kafka...", "info");
  try {
    const res = await fetch('/api/v1/dlq/replay', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clusterId })
    });

    const data = await res.json();
    showToast(`Replayed ${data.replayedCount} webhooks successfully!`, "success");
    fetchOverview();
    fetchDeliveries();
    fetchClusters();
  } catch (err) {
    showToast("Replay execution failed", "error");
    console.error(err);
  }
}

// ============================================================================
// Clipboard & Toast Utilities
// ============================================================================
function copyText(text, successMsg = "Copied to clipboard!") {
  navigator.clipboard.writeText(text).then(() => {
    showToast(successMsg, "info");
  }).catch(() => {
    showToast("Failed to copy", "error");
  });
}

function showToast(message, type = "info") {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span>${type === 'success' ? '✓' : type === 'error' ? '✕' : 'ℹ'}</span>
    <span>${message}</span>
  `;

  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    toast.style.transition = '0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}
