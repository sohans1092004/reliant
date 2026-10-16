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

-- Per-tenant policy. NULL = use the global default (env).
-- Retry schedule: settable by the tenant from the dashboard.
-- Rate limit: operator-only (a tenant must not be able to raise its own limit).
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS retry_schedule_minutes INT[];
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS rate_limit_per_sec INT CHECK (rate_limit_per_sec > 0);
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS rate_limit_burst INT CHECK (rate_limit_burst > 0);

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

-- Unknown tenants are rejected by Postgres inside the ingest INSERT itself.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'events_tenant_fk') THEN
    ALTER TABLE events ADD CONSTRAINT events_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id);
  END IF;
END $$;

-- W3C trace context of the ingest request, so retries hours later join the same trace.
ALTER TABLE events ADD COLUMN IF NOT EXISTS traceparent TEXT;

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
-- Subscriptions (spec FR2): which event types this endpoint receives. NULL = every type.
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS event_types               TEXT[];

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
-- Monthly RANGE partitions on attempted_at (spec §5): retention drops a whole old month
-- instead of deleting it row by row, and dashboard queries only touch recent months.

-- One-time conversion of a table created before partitioning: move it (and the names it
-- owns) aside; its rows are copied into the partitioned table further down.
-- Run with consumers and workers stopped: ledger writes fail between these statements.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'delivery_ledger' AND relkind = 'r') THEN
    ALTER TABLE delivery_ledger RENAME TO delivery_ledger_unpartitioned;
    ALTER SEQUENCE delivery_ledger_id_seq RENAME TO delivery_ledger_unpartitioned_id_seq;
    ALTER INDEX delivery_ledger_pkey RENAME TO delivery_ledger_unpartitioned_pkey;
    ALTER INDEX IF EXISTS ledger_dashboard RENAME TO ledger_dashboard_unpartitioned;
    ALTER INDEX IF EXISTS ledger_pair RENAME TO ledger_pair_unpartitioned;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS delivery_ledger (
  id               BIGSERIAL,
  event_id         UUID NOT NULL REFERENCES events(event_id),
  endpoint_id      UUID NOT NULL REFERENCES endpoints(endpoint_id),
  tenant_id        TEXT NOT NULL,
  attempt_number   INT NOT NULL,
  status           TEXT NOT NULL CHECK (status IN ('delivered', 'failed')),
  http_status_code INT,
  latency_ms       INT,
  error_detail     TEXT,
  attempted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, attempted_at) -- a partitioned table's key must include the partition column
) PARTITION BY RANGE (attempted_at);
CREATE INDEX IF NOT EXISTS ledger_dashboard ON delivery_ledger (tenant_id, endpoint_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS ledger_pair ON delivery_ledger (event_id, endpoint_id, attempted_at DESC);

-- delivery_ledger_YYYY_MM covers one calendar month (UTC).
CREATE OR REPLACE FUNCTION create_ledger_partition(month DATE) RETURNS void LANGUAGE plpgsql AS $$
DECLARE start DATE := date_trunc('month', month)::date;
BEGIN
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF delivery_ledger FOR VALUES FROM (%L) TO (%L)',
    'delivery_ledger_' || to_char(start, 'YYYY_MM'),
    (start::timestamp AT TIME ZONE 'UTC'), ((start + INTERVAL '1 month')::timestamp AT TIME ZONE 'UTC'));
END $$;

-- This month and the next `ahead`. Run by migrate and by every retention run (daily),
-- so a month's partition always exists before its first write.
CREATE OR REPLACE FUNCTION ensure_ledger_partitions(ahead INT DEFAULT 3) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  FOR i IN 0..ahead LOOP
    PERFORM create_ledger_partition((date_trunc('month', NOW() AT TIME ZONE 'UTC') + make_interval(months => i))::date);
  END LOOP;
END $$;

-- Safety net: if partition maintenance stops running, writes land here instead of failing
-- (a failed ledger write stalls consumption). Rows here block creating their month's
-- partition later, so this should stay empty: the retention job warns when it isn't.
CREATE TABLE IF NOT EXISTS delivery_ledger_default PARTITION OF delivery_ledger DEFAULT;
SELECT ensure_ledger_partitions();

DO $$
DECLARE m DATE;
BEGIN
  IF to_regclass('delivery_ledger_unpartitioned') IS NOT NULL THEN
    FOR m IN SELECT DISTINCT date_trunc('month', attempted_at AT TIME ZONE 'UTC')::date FROM delivery_ledger_unpartitioned LOOP
      PERFORM create_ledger_partition(m);
    END LOOP;
    INSERT INTO delivery_ledger (id, event_id, endpoint_id, tenant_id, attempt_number, status, http_status_code, latency_ms, error_detail, attempted_at)
      SELECT id, event_id, endpoint_id, tenant_id, attempt_number, status, http_status_code, latency_ms, error_detail, attempted_at
      FROM delivery_ledger_unpartitioned;
    PERFORM setval(pg_get_serial_sequence('delivery_ledger', 'id'), GREATEST((SELECT max(id) FROM delivery_ledger), 1));
    DROP TABLE delivery_ledger_unpartitioned;
  END IF;
END $$;

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
