# Reliant

Webhook delivery with durable accountability: every accepted event is delivered, retrying on a known schedule, or dead-lettered and replayable. Never silently lost. Spec: `../# Reliant — Engineering Specification.md`.

## Run it

```sh
docker compose up -d          # Postgres, Redis, Redpanda
cp .env.example .env          # fill in DB creds, SECRET_ENCRYPTION_KEY, INGEST_TOKEN
npm install
npm run migrate               # schema + Kafka topics (12 partitions; grows only at zero lag) + signing secrets (get one via "Rotate secret")

npm run ingest                # :3000  POST /events
npm run consumer              # Kafka -> inline delivery attempt
npm run worker                # retries, run as many as you like
npm run retry-publisher       # re-publishes events whose Kafka publish failed
npm run dashboard             # :3001  UI + API, dev keys: dev_key_tenant_a / dev_key_tenant_b

node producer.js              # send a test event (TEST_TENANT_ID=tenant_b for the slow one)
npm run retention             # dry run; --apply archives (archive/*.jsonl.gz) then deletes events > RETENTION_DAYS (35)
```

## Containers and Kubernetes

One image runs every process; each Kubernetes Deployment just runs a different command.

```sh
docker build -t reliant:dev .
docker run --rm reliant:dev node consumer.js        # any of the five processes

# Local cluster (Docker Desktop: Settings -> Kubernetes -> Enable)
cp k8s/local/secrets.env.example k8s/local/secrets.env   # fill in values
kubectl apply -k k8s/local                                # app + in-cluster Postgres/Redis/Redpanda
kubectl -n reliant port-forward svc/dashboard 3001       # http://localhost:3001

# Autoscaling (ADR-006): install KEDA once, then
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.21.0/keda-2.21.0.yaml
kubectl apply -k k8s/local/autoscaling                    # consumer on Kafka lag, worker on due retries
```

- `k8s/app/`: Reliant itself, environment-agnostic. Five Deployments, ingest and dashboard Services, and a `migrate` Job that is safe to re-run. Every pod runs non-root with a read-only root filesystem, drops all capabilities, and has probes and resource requests.
- `k8s/local/`: an overlay that adds single-replica Postgres, Redis and Redpanda, plus config and secrets. An AWS overlay would point the same `k8s/app` at RDS, ElastiCache and MSK.
  - **Dev only, no persistence:** restarting the Postgres or Redpanda pod wipes them. After that, run `kubectl -n reliant delete job migrate --ignore-not-found && kubectl apply -k k8s/local` to recreate the schema and topics. The migrate Job deletes itself 30s after finishing, so re-applying re-runs it.
  - **Addresses are fully qualified** (`*.reliant.svc.cluster.local`), so KEDA's scalers in the `keda` namespace can reach them too.
- Every process shuts down gracefully on SIGTERM (`lib/shutdown.js`): it stops taking new work, finishes what's in flight, and force-exits at 25s, inside Kubernetes' 30s grace period. Verified in Docker: a worker sent SIGTERM mid-delivery finished the delivery, recorded it in the ledger, and exited with code 0 after 3.2s.

## Tests

```sh
npm test                      # core: backoff, circuit breaker, signing, validation (no I/O)
npm run test:integration      # needs docker compose up and no consumer/worker running:
                              # tenant isolation + full pipeline end to end
npm run loadtest -- ingest    # also: isolation, recovery. LT_INGEST_PROCS=4 for multi-process
```

Results and bottleneck analysis: [`docs/load-test-results.md`](docs/load-test-results.md).

## Observability

- **Metrics:** each process serves Prometheus metrics on an internal port (never the public dashboard port): ingest `:9100`, consumer `:9101`, worker `:9102`, dashboard `:9103`, retry-publisher `:9104`. Override with `METRICS_PORT`, or set `0` to disable, e.g. for a second worker on the same host. Definitions are in `lib/metrics.js`; the worker also reports system-wide DLQ depth, pending backlog and open breakers.
- **Logs:** one JSON object per line (`lib/log.js`), carrying `event_id` / `endpoint_id` / `tenant_id` wherever they're known.
- **Traces (OpenTelemetry):** set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` and each event gets one trace across processes: `ingest` → `kafka.publish` → `webhook.post` (consumer) → `deliver.retry` / `webhook.post` / `ledger.write` (worker, even hours later). The context travels in Kafka headers and in `events.traceparent`. With the variable unset, tracing is off and costs nothing.
- **Stack:** `docker compose --profile observability up -d` starts Prometheus (http://localhost:9090, alert rules in `observability/alerts.yml`), Jaeger (http://localhost:16686) and Grafana (http://localhost:3002, with the provisioned *Reliant overview* dashboard and Jaeger in Explore). The profile is opt-in, so plain `docker compose up` and CI are unaffected.
- **Alerts:** DLQ depth and growth, consumer lag growing, p95 latency above 5s, retry queue stuck, a breaker open for more than 30 minutes (per endpoint), per-tenant success rate, and a process down.

**CI** (`.github/workflows/ci.yml`) runs on every push to `main` and every pull request. Core unit tests run first, as a fast gate. If they pass, the integration tests run against the same `docker-compose.yml` stack, with test-only secrets generated fresh for each run. Load tests are not part of CI.

## Layout

| Path | What |
|---|---|
| `core/` | Pure logic, no I/O (spec §4 / Phase 1) |
| `lib/infra.js` | Shared Postgres / Kafka / Redis clients, config, topic setup |
| `lib/deliver.js` | One delivery attempt: SSRF check, HMAC sign, POST, ledger row, breaker update |
| `lib/breaker.js` | Circuit-breaker state in Redis |
| `ingest-api.js` | Validate, dedup, archive in Postgres, publish to Kafka, then 202 |
| `consumer.js` | Batched inline attempts (1s cap) or queue in `delivery_attempts` |
| `delivery-worker.js` | Leased retries, oldest-first per endpoint, backoff, DLQ |
| `dashboard-api.js` + `public/` | Tenant API + UI |
| `schema.sql` | Idempotent schema, also applied by `npm run migrate` |
| `retention.js` | Archive-then-delete of events past the replay window |
| `lib/rate-limit.js` | Per-tenant token bucket (atomic Lua script in Redis) |

**Per-tenant policy:** tenants set their own retry schedule from the dashboard (`PUT /api/settings`, 1–10 retries of 1–1440 min). Operators set rate limits in the database (`tenants.rate_limit_per_sec`, `rate_limit_burst`); `NULL` means the env default.

## Receiver contract

Every request is a signed `POST` with `webhook-id` (= `event_id`), `webhook-timestamp` and `webhook-signature` (`v1,<base64 HMAC-SHA256 of id.timestamp.body>`; two signatures, space-separated, during a secret rotation). See `verify()` in `core/signing.js`. Receivers should:

- return 2xx only once the event is handled; anything else is retried
- deduplicate on `webhook-id`, because delivery is at-least-once
- answer `type: "reliant.ping"` with 2xx and do nothing else; Reliant uses it to tell a broken endpoint from one bad event

## Where this deviates from the spec

Decisions and their trade-offs are in [`docs/adr/`](docs/adr/README.md). The two that differ from the spec:

- **[ADR-002](docs/adr/0002-retry-queue-in-postgres.md):** retries use a Postgres queue (`delivery_attempts`), not Kafka retry topics.
- **[ADR-008](docs/adr/0008-ordering-guarantee.md):** delivery is completion-ordered per endpoint, not initiation-ordered. If an event fails 3 times, the endpoint is pinged. If the ping succeeds, the event is the problem: it's dead-lettered as `payload_rejected` (replayable) so it stops blocking the queue.

Smaller deviations:

- **A clean inline success writes only a ledger row, not a `delivery_attempts` row.** The status API falls back to the ledger.
- **Ingest dedups on the `events.event_id` primary key only.** There's no Redis fast path in front of it, since the PK index check is already cheap.
- **Not built (needs AWS / real infra):** Kubernetes, Terraform, CI/CD, OpenTelemetry/metrics, ledger time-partitioning, per-tenant retry policies.
