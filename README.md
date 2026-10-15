# Reliant

Webhook delivery with durable accountability: every accepted event is delivered, retrying on a known schedule, or dead-lettered and replayable. Never silently lost. Spec: `../# Reliant — Engineering Specification.md`.

## Run it

```sh
docker compose up -d          # Postgres, Redis, Redpanda
cp .env.example .env          # fill in DB creds, SECRET_ENCRYPTION_KEY, INGEST_TOKEN
npm install
npm run migrate               # schema + Kafka topics + signing secrets (printed once)

npm run ingest                # :3000  POST /events
npm run consumer              # Kafka -> inline delivery attempt
npm run worker                # retries, run as many as you like
npm run retry-publisher       # re-publishes events whose Kafka publish failed
npm run dashboard             # :3001  UI + API, dev keys: dev_key_tenant_a / dev_key_tenant_b

node producer.js              # send a test event (TEST_TENANT_ID=tenant_b for the slow one)
```

## Tests

```sh
npm test                      # core: backoff, circuit breaker, signing, validation (no I/O)
npm run test:integration      # needs docker compose up and no consumer/worker running:
                              # tenant isolation + full pipeline end to end
```

## Layout

| Path | What |
|---|---|
| `core/` | Pure logic, no I/O (spec §4 / Phase 1) |
| `lib/infra.js` | Shared Postgres / Kafka / Redis clients, config, topic setup |
| `lib/deliver.js` | One delivery attempt: SSRF check, HMAC sign, POST, ledger row, breaker update |
| `lib/breaker.js` | Circuit-breaker state in Redis |
| `ingest-api.js` | Validate, dedup, archive in Postgres, publish to Kafka, then 202 |
| `consumer.js` | Inline attempt (3s cap) or queue in `delivery_attempts` |
| `delivery-worker.js` | Leased retries, oldest-first per endpoint, backoff, DLQ |
| `dashboard-api.js` + `public/` | Tenant API + UI |
| `schema.sql` | Idempotent schema, also applied by `npm run migrate` |

## Where this deviates from the spec (decide and write up as ADRs)

- **Retries use a Postgres queue (`delivery_attempts`), not Kafka retry topics.** ADR-002 picked retry topics. The Postgres queue came out of the Day 2 slow-endpoint work: it separates delivery concurrency from Kafka partitions and gives strict per-endpoint ordering via `DISTINCT ON`. The cost is polling load on Postgres.
- **A clean inline success writes only a ledger row, not a `delivery_attempts` row.** The status API falls back to the ledger.
- **Ingest dedups on the `events.event_id` primary key only.** There's no Redis fast path in front of it, since the PK index check is already cheap.
- **Not built (needs AWS / real infra):** Kubernetes, Terraform, CI/CD, load tests (§14), OpenTelemetry/metrics, ledger time-partitioning, per-tenant retry policies.
