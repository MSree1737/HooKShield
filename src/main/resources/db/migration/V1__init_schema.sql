-- HookShield Database Initialization & Schema Definition
-- PostgreSQL 16+ with pgvector extension

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_cast c
        JOIN pg_type s ON c.castsource = s.oid
        JOIN pg_type t ON c.casttarget = t.oid
        WHERE s.typname = 'varchar' AND t.typname = 'vector'
    ) THEN
        CREATE CAST (character varying AS vector) WITH INOUT AS IMPLICIT;
    END IF;
END $$;

-- 1. Multi-Tenant Master Table
CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    rate_limit_per_second INT NOT NULL DEFAULT 500,
    burst_capacity INT NOT NULL DEFAULT 1000,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tenant Provider Configuration (Secrets, Signature Headers)
CREATE TABLE IF NOT EXISTS tenant_provider_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    provider_name VARCHAR(50) NOT NULL, -- e.g., 'STRIPE', 'SHOPIFY', 'GITHUB'
    hmac_secret VARCHAR(255) NOT NULL,
    header_signature_key VARCHAR(100) NOT NULL, -- e.g., 'X-Hub-Signature-256', 'Stripe-Signature'
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_tenant_provider UNIQUE (tenant_id, provider_name)
);

-- 3. Webhook Deliveries (Ingestion & Dispatch Audit Log)
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    provider VARCHAR(50) NOT NULL,
    external_event_id VARCHAR(150),
    idempotency_hash VARCHAR(64) NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    status VARCHAR(30) NOT NULL, -- 'RECEIVED', 'DISPATCHED', 'RETRYING', 'DEAD_LETTER', 'REPLAYED'
    retry_count INT NOT NULL DEFAULT 0,
    target_endpoint VARCHAR(500) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 0
);

-- 4. Semantic Error Clusters (pgvector 1536-dim embeddings for failure signatures)
CREATE TABLE IF NOT EXISTS error_clusters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_name VARCHAR(200) NOT NULL,
    failure_category VARCHAR(50) NOT NULL, -- 'EPHEMERAL_NETWORK', 'SCHEMA_MISMATCH', 'AUTH_FAILURE', 'INTERNAL_SERVER_ERROR'
    signature_embedding vector(1536) NOT NULL,
    recommended_action TEXT,
    is_safe_to_autoreplay BOOLEAN NOT NULL DEFAULT FALSE,
    total_occurrences INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Dead-Letter Incident Reports (Triage & Diagnostic Analysis)
CREATE TABLE IF NOT EXISTS dlq_incident_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id UUID NOT NULL REFERENCES webhook_deliveries(id) ON DELETE CASCADE,
    cluster_id UUID REFERENCES error_clusters(id),
    http_status_code INT,
    raw_error_message TEXT,
    ai_root_cause_analysis TEXT,
    ai_confidence_score INT,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for high-throughput lookup & analytics
CREATE INDEX IF NOT EXISTS idx_deliveries_tenant_status ON webhook_deliveries (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_deliveries_created_at ON webhook_deliveries (created_at);
CREATE INDEX IF NOT EXISTS idx_error_clusters_embedding ON error_clusters USING ivfflat (signature_embedding vector_cosine_ops) WITH (lists = 100);
