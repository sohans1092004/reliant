# Reliant

Webhook delivery with durable accountability: every accepted event is delivered, retrying on a known schedule, or dead-lettered and replayable. Never silently lost. Spec: `../# Reliant — Engineering Specification.md`.

## Run it

```sh
docker compose up -d          # Postgres, Redis, Redpanda
cp .env.example .env          # fill in DB creds, SECRET_ENCRYPTION_KEY, INGEST_TOKEN
npm install
npm run migrate               # schema + Kafka topics (12 partitions; grows only at zero lag) + signing secrets (get one via "Rotate secret")
                              # + dev tenants tenant_a / tenant_b, because .env has DEV_SEED=true (never on a public server)

npm run ingest                # :3000  POST /events
npm run consumer              # Kafka -> inline delivery attempt
npm run worker                # retries, run as many as you like
npm run retry-publisher       # re-publishes events whose Kafka publish failed; reconciler (below)
npm run create-user -- tenant_a you@example.com owner   # prints a password once
npm run dashboard             # :3001  sign in with it; scripts can use API keys (dev_key_tenant_a / _b)

node producer.js              # send a test event (TEST_TENANT_ID=tenant_b for the slow one)
npm run retention             # dry run; --apply archives (archive/*.jsonl.gz) then deletes events > RETENTION_DAYS (35)
```

**Live on one VM:** [`deploy/`](deploy/README.md) runs the whole stack with `docker compose` behind Caddy (automatic HTTPS), with a demo receiver and the public-exposure settings fixed. Tenants there come from `npm run create-tenant -- <id> "<name>"`, which prints the API key once.

## AWS: KMS and S3 (against LocalStack)

```sh
docker compose --profile aws up -d    # LocalStack: KMS key alias/reliant-secrets, bucket reliant-archive
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export KMS_KEY_ID=alias/reliant-secrets ARCHIVE_BUCKET=reliant-archive
node --test tests/aws.test.js         # CI runs every integration test this way
```

- **Endpoint secrets, envelope encryption:** with `KMS_KEY_ID` set, each secret is AES-256-GCM-encrypted under its own data key from KMS `GenerateDataKey`, and only the KMS-wrapped copy of that key is stored (`kms1:<wrapped key>:<ciphertext>`). The master key never leaves KMS, and revoking `kms:Decrypt` revokes every secret at once. The data key is bound to an encryption context, so KMS won't unwrap it for any other purpose. Decrypted secrets are cached by ciphertext, so KMS sees one call per secret per process, not per delivery.
- **Key versioning:** the `kms1:` prefix tells a blob's format. Blobs without it are under `SECRET_ENCRYPTION_KEY`; `npm run migrate` with `KMS_KEY_ID` set re-wraps them (`rewrapSecrets`), and the KMS key itself rotates yearly (`enable-key-rotation`) without re-encrypting anything.
- **Archives:** with `ARCHIVE_BUCKET` set, retention uploads each batch to `s3://<bucket>/events/YYYY-MM-DD/`, encrypted at rest under the KMS key (SSE-KMS), before deleting the rows.
- **Don't keep `KMS_KEY_ID` in `.env` locally:** LocalStack's free edition keeps no state, so after a restart its key is gone and secrets wrapped under it can't be decrypted (rotate them to recover).

## AWS infrastructure (Terraform)

[`infra/terraform/`](infra/terraform/README.md) creates the AWS version of the stack:
- a VPC over 3 AZs
- EKS with Pod Identity, plus an ECR repository
- RDS Postgres 16 (Multi-AZ, TLS, point-in-time recovery)
- ElastiCache Redis and MSK, both with TLS (ADR-003)
- the KMS key and archive bucket
- one least-privilege IAM role per process

[`k8s/aws/`](k8s/aws/kustomization.yaml) runs the same `k8s/app` on it: service accounts bound to those roles, the RDS CA bundle, and a daily retention CronJob.

None of it has been applied to a real account. CI checks it with `terraform fmt`, `validate`, and a `plan` against LocalStack (100 resources), and renders the overlay. The KMS, S3 and IAM parts have been applied to LocalStack and tested there.

The app reads three settings for the AWS services:
- `KAFKA_SSL=true` and a comma-separated `KAFKA_BROKER` list for MSK
- `DB_SSL_CA` for RDS TLS with certificate verification
- a `rediss://` URL for ElastiCache TLS

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
- `k8s/local/`: an overlay that adds single-replica Postgres, Redis and Redpanda, plus config and secrets. `redpanda-bootstrap.yaml` (also used by docker compose) turns Redpanda's write caching off, so a Kafka ack means the event is on disk, and turns off two Enterprise-licensed features the image enables by default. `k8s/aws/` points the same `k8s/app` at RDS, ElastiCache and MSK (see AWS infrastructure above).
  - **Dev only, no persistence:** restarting the Postgres or Redpanda pod wipes them. After that, run `kubectl -n reliant delete job migrate --ignore-not-found && kubectl apply -k k8s/local` to recreate the schema and topics. The migrate Job deletes itself 30s after finishing, so re-applying re-runs it.
  - **Addresses are fully qualified** (`*.reliant.svc.cluster.local`), so KEDA's scalers in the `keda` namespace can reach them too.
- Every process shuts down gracefully on SIGTERM (`lib/shutdown.js`): it stops taking new work, finishes what's in flight, and force-exits at 25s, inside Kubernetes' 30s grace period. Verified in Docker: a worker sent SIGTERM mid-delivery finished the delivery, recorded it in the ledger, and exited with code 0 after 3.2s.

## Tests

```sh
npm test                      # core: backoff, circuit breaker, signing, validation (no I/O)
npm run test:integration      # needs docker compose up and no consumer/worker running:
                              # tenant isolation + full pipeline end to end
npm run loadtest -- ingest    # also: isolation, recovery. LT_INGEST_PROCS=4 for multi-process
npm run loadtest -- recovery-spec  # spec benchmark: 600k backlog drained within 5 min (LT_CONSUMERS)
npm run loadtest -- dashboard # dashboard query latency over 1M ledger rows
npm run loadtest -- pg-outage # kill Postgres mid-traffic (docker on PATH); also redis-outage, kafka-outage
bash loadtest/k8s-drill.sh pg-outage   # the same Postgres drill against the k8s/local deployment (also k8s-scale-out, k8s-rolling-restart)
LT_SINK_HOST=host.docker.internal npm run loadtest -- consumer-freeze  # zombie consumer (needs reliant:dev image)
npm run loadtest -- redis-hang  # Redis down while every tenant also has a hanging endpoint
npm run loadtest -- retry-drain # retry-lane throughput: one worker draining 100 endpoints x LT_ROWS queued rows
LT_SINK_HOST=host.docker.internal npm run loadtest -- lease-double-send   # worker frozen mid-POST past its lease
LT_SINK_HOST=host.docker.internal npm run loadtest -- consumer-partition  # one consumer cut off the broker, still running
```

Results and bottleneck analysis: [`docs/load-test-results.md`](docs/load-test-results.md). Failure drills, round 2: [`docs/drills.md`](docs/drills.md).
Induced-failure postmortems: [`production/incident-reports/`](production/incident-reports/).

## Observability

- **Metrics:** each process serves Prometheus metrics on an internal port (never the public dashboard port): ingest `:9100`, consumer `:9101`, worker `:9102`, dashboard `:9103`, retry-publisher `:9104`. Override with `METRICS_PORT`, or set `0` to disable, e.g. for a second worker on the same host. Definitions are in `lib/metrics.js`; the worker also reports system-wide DLQ depth, pending backlog and open breakers.
- **Logs:** one JSON object per line (`lib/log.js`), carrying `event_id` / `endpoint_id` / `tenant_id` wherever they're known.
- **Traces (OpenTelemetry):** set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` and each event gets one trace across processes: `ingest` → `kafka.publish` → `webhook.post` (consumer) → `deliver.retry` / `webhook.post` / `ledger.write` (worker, even hours later). The context travels in Kafka headers and in `events.traceparent`. With the variable unset, tracing is off and costs nothing.
- **Stack:** `docker compose --profile observability up -d` starts Prometheus (http://localhost:9090, alert rules in `observability/alerts.yml`), Jaeger (http://localhost:16686) and Grafana (http://localhost:3002, with the provisioned *Reliant overview* dashboard and Jaeger in Explore). The profile is opt-in, so plain `docker compose up` and CI are unaffected.
- **Kubernetes:** `kubectl apply -k observability` runs a Prometheus inside the cluster that finds every Reliant pod (`prometheus.io/scrape` annotations), with the same alert rules and a read-only role limited to listing pods in `reliant`. To see it in the local Grafana: `docker compose -f docker-compose.yml -f observability/compose.k8s.yaml --profile observability up -d reliant_grafana`, then pick **Prometheus (Kubernetes)** in the dashboard's Prometheus menu.
- **Live stack:** `deploy/compose.monitoring.yaml` adds Prometheus and Grafana to the `deploy/` stack (see `deploy/README.md`). Same rules, same dashboard; Grafana listens on 127.0.0.1 only and needs a password.
- **Alerts** (12): accepted events with no outcome after 15 minutes, a worker losing its lease, an endpoint disabled after a 410, DLQ depth and growth, consumer lag growing, p95 latency above 5s, retry queue stuck, a breaker open for more than 30 minutes (per endpoint), per-tenant success rate, ingest 5xx, and a process down.
- **Reconciler** (in `retry-publisher.js`, every 5 min): finds events accepted 15–60 minutes ago that have a subscribed endpoint but no ledger row and no queued delivery, which ADR-005 says can't happen. It exports `reliant_unaccounted_events` and, once the consumer group's lag is under 1,000, sends them back to Kafka. During a backlog it only reports, since those events are usually still waiting in Kafka.

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
| `lib/auth.js`, `core/password.js` | Dashboard sign-in: sessions, API keys, scrypt passwords |
| `schema.sql` | Idempotent schema, also applied by `npm run migrate` |
| `retention.js` | Archive-then-delete of events past the replay window |
| `lib/rate-limit.js` | Per-tenant token bucket (atomic Lua script in Redis) |

## Dashboard sign-in

People sign in with email and password; scripts keep using a tenant API key (`Authorization: Bearer`), which acts as the tenant's owner.

- **Roles:** an *owner* manages endpoints, secrets, replays, settings and users. A *viewer* can read everything but change nothing; the server refuses, and the UI hides the controls. A tenant always keeps at least one owner.
- **Users:** the first owner comes from `npm run create-user`. After that, owners add users from the dashboard. A new user's password is generated and shown once, like a signing secret, and they change it after signing in. Changing a password signs out the user's other sessions.
- **Passwords** are hashed with scrypt (Node's crypto, no dependency). Policy: 12 to 200 characters, and the password must not contain the email's name. An unknown email takes as long to reject as a wrong password, so timing doesn't reveal who has an account.
- **Sessions:** a random 256-bit token in a cookie that is `HttpOnly`, `SameSite=Strict` and `Secure`, valid for 12 hours. Postgres stores only its SHA-256, so a leaked `sessions` table can't be replayed as logins.
  - `Secure` works on `http://localhost`. Set `COOKIE_SECURE=false` only for another plain-HTTP host.
- **Cross-site request forgery:** writes made with the cookie must be same-origin JSON. That covers login too, so another site can't sign a visitor into the attacker's account.
- **Brute force:** login allows 5 attempts per email, then 1 a minute, and 30 per IP, then 1 every 10 seconds. It uses the Redis token bucket and, like ingest's limiter, fails open if Redis is down. Behind a load balancer, set `TRUST_PROXY=true` so the per-IP limit uses the client's address.

**Per-tenant policy:** tenants set their own retry schedule from the dashboard (`PUT /api/settings`, 1–10 retries of 1–1440 min). Operators set rate limits in the database (`tenants.rate_limit_per_sec`, `rate_limit_burst`); `NULL` means the env default.

## Receiver contract

Every request is a signed `POST` with `webhook-id` (= `event_id`), `webhook-timestamp` and `webhook-signature` (`v1,<base64 HMAC-SHA256 of id.timestamp.body>`, keyed with the base64-decoded part of the `whsec_` secret; two signatures, space-separated, during a secret rotation). See `verify()` in `core/signing.js`. Receivers should:

- return 2xx only once the event is handled; anything else is retried
- to slow Reliant down, answer `429` (or `503`) with `Retry-After`: the next try waits that long (up to 24h)
- answer `410 Gone` to stop deliveries for good: Reliant disables the endpoint and dead-letters what was queued for it; an owner can turn it back on and replay ([ADR-009](docs/adr/0009-status-codes-and-dead-endpoint-backlogs.md))
- deduplicate on `webhook-id`, because delivery is at-least-once
- answer `type: "reliant.ping"` with 2xx and do nothing else; Reliant uses it to tell a broken endpoint from one bad event

**Python receiver SDK** ([`sdk/python/`](sdk/python/README.md)): verifies signatures (both secrets during a rotation), rejects replays, skips `reliant.ping`, and dedups on `webhook-id`. Standard library only. Its tests sign with `core/signing.js`, so they catch any drift between SDK and server.

**Standard Webhooks compatible:** Reliant signs exactly as [Standard Webhooks](https://www.standardwebhooks.com/) specifies (same headers; the HMAC key is the base64-decoded part after `whsec_`), so receivers can use its official libraries in any language. Checked against the published test vector (`core/core.test.js`) and, in CI, against the official Python library in both directions.

## Where this deviates from the spec

Decisions and their trade-offs are in [`docs/adr/`](docs/adr/README.md). The two that differ from the spec:

- **[ADR-002](docs/adr/0002-retry-queue-in-postgres.md):** retries use a Postgres queue (`delivery_attempts`), not Kafka retry topics.
- **[ADR-008](docs/adr/0008-ordering-guarantee.md):** delivery is completion-ordered per endpoint, not initiation-ordered. If an event fails 3 times, the endpoint is pinged. If the ping succeeds, the event is the problem: it's dead-lettered as `payload_rejected` (replayable) so it stops blocking the queue. If the ping fails too and the event runs out of retries, the events queued behind it go to the DLQ with it, so none waits more than one retry cycle ([ADR-009](docs/adr/0009-status-codes-and-dead-endpoint-backlogs.md)).

Smaller deviations:

- **A clean inline success writes only a ledger row, not a `delivery_attempts` row.** The status API falls back to the ledger.
- **Ingest dedups on the `events.event_id` primary key only.** There's no Redis fast path in front of it, since the PK index check is already cheap.
- **Subscriptions are an `event_types` list on the endpoint** (NULL = every type), not a separate `subscriptions` table: same capability, one fewer join on the hot path.
- **Dead letters are `delivery_attempts` rows with `status = 'dead'`**, not a separate `dead_letters` table, and there's no `delivery_status` projection table: `delivery_attempts` plus the ledger answer the status API (p95 13–23ms over 1M ledger rows, `docs/load-test-results.md` §6).
- **Ledger partitions older than the cutoff are dropped once empty**, rather than archived per partition: retention already archives each event with its ledger rows (to S3 with `ARCHIVE_BUCKET`, else local gzip files).
- **AWS is written and checked, never applied to a real account** (that costs money): Terraform for the whole stack and a `k8s/aws` overlay (below). KMS and S3 are also exercised by the app's tests against LocalStack.
- **Not built:** the deploy stage of CI/CD (push to ECR, apply to EKS), dashboard exposure (ALB + certificate), KEDA on EKS.
