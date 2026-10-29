-- PostgreSQL Multi-Tenant Auth & Rate-Limiting Control Plane Schema
CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE,
    plan_tier VARCHAR(32) NOT NULL DEFAULT 'pro',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(128) NOT NULL,
    key_prefix VARCHAR(16) NOT NULL,
    key_hash VARCHAR(64) NOT NULL UNIQUE,
    scopes VARCHAR(256) NOT NULL DEFAULT 'ingest:write,analytics:read',
    rate_limit_per_min INTEGER NOT NULL DEFAULT 600000,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id ON api_keys(tenant_id);

-- Seed default tenant & benchmark API key (raw key: sp_live_default_bench_key_2026)
-- SHA-256("sp_live_default_bench_key_2026") = 7605d99a4e5861fa72da7af08e987f06f4b954b9968a99dfc918050151807918
INSERT INTO tenants (id, name, plan_tier, is_active)
VALUES ('tenant_default', 'StreamPulse Default Org', 'enterprise', TRUE)
ON CONFLICT (id) DO NOTHING;

INSERT INTO api_keys (id, tenant_id, name, key_prefix, key_hash, scopes, rate_limit_per_min, is_active)
VALUES (
    'key_default_bench',
    'tenant_default',
    'Default Benchmark & Verifier Key',
    'sp_live_defa',
    '7605d99a4e5861fa72da7af08e987f06f4b954b9968a99dfc918050151807918',
    'ingest:write,analytics:read,admin',
    1000000,
    TRUE
)
ON CONFLICT (id) DO NOTHING;
