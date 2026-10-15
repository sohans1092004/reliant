-- Reliant schema. Idempotent: safe to re-run on a fresh OR existing DB (npm run migrate).
-- Also auto-applied by docker-compose on a fresh (empty) Postgres volume - but run
-- `npm run migrate` afterwards anyway, it also backfills endpoint signing secrets.

-- Tenants. api_key_hash = sha256 hex of the tenant's dashboard API key (never stored raw).
CREATE TABLE IF NOT EXISTS tenants (
  tenant_id    TEXT PRIMARY KEY,
  name         TEXT NOT NULL,
  api_key_hash TEXT NOT NULL UNIQUE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Durable event archive - independent of Kafka retention, which is what makes
-- replaying a 3-week-old event possible (ADR-001). Rows are never deleted.
-- status here only means "reached Kafka", NOT "reached the customer".
CREATE TABLE IF NOT EXISTS events (
  event_id           UUID PRIMARY KEY,   -- PK doubles as the ingest dedup backstop
  type               TEXT NOT NULL,
  payload            JSONB NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL,
  published_to_kafka BOOLEAN NOT NULL DEFAULT FALSE,
  -- retry-publisher picks up rows where status = 'pending' AND next_retry_at <= NOW(),
  -- so these defaults are what make a failed Kafka publish get retried at all.
  status             TEXT NOT NULL DEFAULT 'pending',
  attempt_count      INT NOT NULL DEFAULT 0,
  next_retry_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  tenant_id          TEXT NOT NULL
);

-- Where a tenant's events currently go. One tenant can have many endpoints.
CREATE TABLE IF NOT EXISTS endpoints (
  endpoint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   TEXT NOT NULL,
  url         TEXT NOT NULL,
  is_active   BOOLEAN NOT NULL DEFAULT TRUE
);
-- Signing secrets, AES-GCM encrypted (core/signing.js). previous + rotated_at = dual-secret rotation window.
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS secret_encrypted          TEXT;
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS secret_previous_encrypted TEXT;
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS secret_rotated_at         TIMESTAMPTZ;
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Current delivery state per (event, endpoint) - the "what happened to my webhook" projection.
-- Only written when the inline attempt didn't succeed (or on replay); a clean inline
-- success lives only in delivery_ledger.
CREATE TABLE IF NOT EXISTS delivery_attempts (
  id            BIGSERIAL PRIMARY KEY,
  event_id      UUID NOT NULL REFERENCES events(event_id),
  endpoint_id   UUID NOT NULL REFERENCES endpoints(endpoint_id),
  status        TEXT NOT NULL DEFAULT 'pending',   -- pending | delivered | dead
  attempt_count INT NOT NULL DEFAULT 0,            -- attempts made so far, inline one included
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error    TEXT
);
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS last_http_status  INT;
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS dead_reason       TEXT;  -- delivery_exhausted | payload_rejected | processing_error
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS terminal_at       TIMESTAMPTZ;
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS locked_until      TIMESTAMPTZ;  -- worker lease
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS processing_errors INT NOT NULL DEFAULT 0;
-- One row per pair: lets replay be an upsert and makes a redelivered Kafka message a no-op.
CREATE UNIQUE INDEX IF NOT EXISTS delivery_attempts_pair ON delivery_attempts (event_id, endpoint_id);
-- Backs the worker's "oldest pending row per endpoint" query.
CREATE INDEX IF NOT EXISTS delivery_attempts_pending ON delivery_attempts (endpoint_id, created_at, id) WHERE status = 'pending';

-- Append-only audit trail: one row per HTTP attempt, inline or retry. Never updated.
-- ponytail: not time-partitioned yet; add monthly RANGE partitions when it gets big.
CREATE TABLE IF NOT EXISTS delivery_ledger (
  id               BIGSERIAL PRIMARY KEY,
  event_id         UUID NOT NULL REFERENCES events(event_id),
  endpoint_id      UUID NOT NULL REFERENCES endpoints(endpoint_id),
  tenant_id        TEXT NOT NULL,
  attempt_number   INT NOT NULL,
  status           TEXT NOT NULL CHECK (status IN ('delivered', 'failed')),
  http_status_code INT,
  latency_ms       INT,
  error_detail     TEXT,
  attempted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ledger_dashboard ON delivery_ledger (tenant_id, endpoint_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS ledger_pair ON delivery_ledger (event_id, endpoint_id, attempted_at DESC);

-- Dev seed. Dashboard API keys: dev_key_tenant_a / dev_key_tenant_b. Local only.
INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES
  ('tenant_a', 'Tenant A', encode(sha256('dev_key_tenant_a'::bytea), 'hex')),
  ('tenant_b', 'Tenant B', encode(sha256('dev_key_tenant_b'::bytea), 'hex'))
ON CONFLICT DO NOTHING;

-- tenant_b points at fake-slow-endpoint.js. Replace tenant_a's URL with your webhook.site one.
INSERT INTO endpoints (tenant_id, url)
SELECT * FROM (VALUES
  ('tenant_a', 'https://webhook.site/REPLACE-ME'),
  ('tenant_b', 'http://localhost:9998')
) AS seed(tenant_id, url)
WHERE NOT EXISTS (SELECT 1 FROM endpoints);
