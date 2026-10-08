-- HookShield Initial Tenant & Provider Seed Data

INSERT INTO tenants (id, name, rate_limit_per_second, burst_capacity)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'Acme Corporation', 500, 1000),
    ('00000000-0000-0000-0000-000000000002', 'Global Logistics Inc', 1000, 2000)
ON CONFLICT (name) DO NOTHING;

INSERT INTO tenant_provider_configs (id, tenant_id, provider_name, hmac_secret, header_signature_key, is_active)
VALUES
    ('10000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', 'STRIPE', 'whsec_stripe_test_secret_key_123', 'Stripe-Signature', true),
    ('10000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'GITHUB', 'whsec_github_test_secret_key_456', 'X-Hub-Signature-256', true),
    ('10000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'SHOPIFY', 'whsec_shopify_test_secret_key_789', 'X-Shopify-Hmac-Sha256', true)
ON CONFLICT (tenant_id, provider_name) DO NOTHING;
