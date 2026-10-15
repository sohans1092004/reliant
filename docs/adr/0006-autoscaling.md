# ADR-006: Autoscale consumer and worker on queue depth (KEDA), not CPU or a fixed pool

**Status:** Accepted (Day 5). Verified on a local cluster for the consumer; the worker's scaler is configured and healthy but hasn't been observed scaling.

## Context

The spec asks for fixed worker pools vs HPA-driven autoscaling, with the delivery worker pool *"autoscaled on consumer lag"* (§2, §13). Load testing (`docs/load-test-results.md`) showed the consumer is CPU-bound at around 800–960 events/s per process, and that throughput scales with consumer processes up to the partition count.

CPU is the wrong signal for both queue-driven processes:
- **Consumer:** a backlog of 600k events and a busy-but-keeping-up consumer look the same on CPU. Lag is what actually says "more consumers needed".
- **Worker:** its work is waiting on slow customer endpoints (I/O), so CPU stays low even when the retry queue is long.

## Decision

Use [KEDA](https://keda.sh) ScaledObjects (`k8s/local/autoscaling/`), each driving a standard HPA:

| Target | Signal | Target per replica | Range |
|---|---|---|---|
| `consumer` | Kafka consumer-group lag on `webhooks.events`, read directly from the broker | 500 messages | 1 – 12 (= partition count; `allowIdleConsumers: false`) |
| `worker` | Endpoints with retry work due now: `count(DISTINCT endpoint_id) ... status='pending' AND next_retry_at <= NOW()` | 50 endpoints | 1 – 10 |
| `ingest` | Not autoscaled locally (fixed 2) | – | – |

- **Worker counts endpoints, not rows.** A worker handles one row per endpoint per sweep (ADR-008 ordering), so 10,000 due rows for one endpoint can't use a second worker, while 10,000 endpoints with one due row each can.
- **Scale down only after 2 quiet minutes** (`horizontalPodAutoscalerConfig.behavior.scaleDown.stabilizationWindowSeconds: 120`), because every consumer scale event triggers a Kafka group rebalance that briefly pauses consumption.
- **Ingest is stateless and CPU-bound**, so plain CPU-based HPA fits it. It needs metrics-server, which the local Docker Desktop cluster doesn't run, so ingest uses a fixed replica count locally; the AWS overlay should add a CPU HPA.
- **The consumer and worker Deployments have no `replicas` field**, so `kubectl apply` doesn't fight the autoscaler.

## Verification (local cluster, Docker Desktop, kind)

The consumer was capped at 0.05 CPU to simulate under-provisioning, then sent a 12,000-event backlog across 30 tenants. KEDA read a lag of 1,611 per replica and **scaled 1 → 4 within ~20s**. That's the HPA's per-step maximum, and the backlog drained before another step was needed. It **scaled back to 1 after the 2-minute window**.

Without the CPU cap, a single consumer drained 5,000 events in seconds, before the HPA's first metric read: on this hardware one consumer usually suffices, which is the point of scaling on lag rather than keeping a fixed large pool.

## Consequences

- KEDA becomes a cluster dependency for autoscaling, installed once per cluster. Without it, the app still runs with 1 consumer and 1 worker.
- KEDA's scalers run in the `keda` namespace, so every address they use must be fully qualified (`postgres.reliant.svc.cluster.local`), and Redpanda must *advertise* its FQDN. Short names caused DNS failures on the first attempt.
- The worker scaler queries Postgres every 15s. It's one cheap indexed count, but it's another reader of the hot table.
- Scaling past 12 consumers needs more partitions first (ADR-004, done only at zero lag).

## Alternatives considered

- **Fixed pool sized for peak:** simplest, but it pays for peak capacity all the time and still falls behind on bigger spikes.
- **CPU HPA for everything:** wrong signal for queue-driven processes (see Context).
- **Prometheus Adapter + HPA on custom metrics** (`reliant_consumer_lag` is already exported): it works, but the scaling then depends on the whole Prometheus pipeline being up and correctly configured. KEDA reads the source of truth (the broker, Postgres) directly.
