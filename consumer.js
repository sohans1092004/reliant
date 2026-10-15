import { pool, kafka, newProducer, ensureTopics, closeRedis, TOPICS, EVENT_PARTITIONS, retryPolicyFor } from "./lib/infra.js";
import { sendSigned, recordAttempts } from "./lib/deliver.js";
import { getBreakers, applyOutcomes } from "./lib/breaker.js";
import * as cb from "./core/circuit-breaker.js";
import { nextRetryDelay } from "./core/backoff.js";
import { validateEvent } from "./core/event.js";
import { log } from "./lib/log.js";
import { metrics, serveMetrics } from "./lib/metrics.js";
import { initTracing, fromTraceparent } from "./lib/tracing.js";
import { context, trace } from "@opentelemetry/api";

const outcomeLabel = (r) => (r.ok ? "delivered" : r.timedOut ? "timeout" : "failed");

// Kept short because Kafka fetches a partition's next batch only after this one is done:
// the first timeout against a newly broken endpoint holds up every tenant on the
// partition (measured: ~3s inline timeout -> ~10s of degraded latency). A timeout here
// only means "slow", so it's retried by the worker right away with a longer timeout.
const INLINE_TIMEOUT_MS = 1000;   // one inline attempt never waits longer than this
const ENDPOINT_BUDGET_MS = 1000;  // per endpoint per chunk: stop STARTING inline attempts after this
const CHUNK_SIZE = 200;           // messages per round of bookkeeping (and per offset commit)

const consumer = kafka.consumer({ groupId: "delivery-workers" });
const dlqProducer = newProducer();

// Poison pill: a message that can never be processed. Retrying would crash-loop the
// partition, so park it on the DLQ topic (there may be no events row to point at).
async function sendToDlq(poisoned, partition) {
  if (!poisoned.length) return;
  await dlqProducer.send({
    topic: TOPICS.dlq,
    messages: poisoned.map(({ message, event, errors }) => ({
      key: event?.tenant_id ?? null,
      value: JSON.stringify({
        reason: "processing_error", errors, partition, offset: message.offset,
        raw: message.value.toString().slice(0, 10_000),
      }),
    })),
  });
  metrics.poisonMessages.inc(poisoned.length);
  for (const p of poisoned) {
    log.warn("unprocessable message sent to DLQ topic", {
      partition, offset: p.message.offset, event_id: p.event?.event_id, tenant_id: p.event?.tenant_id, errors: p.errors,
    });
  }
}

// One chunk of a partition's messages, in Kafka order. The bookkeeping is done per
// chunk instead of per message: ~6 round trips per chunk instead of ~6 per message.
async function processChunk(messages, partition, heartbeat) {
  // 1. Parse + validate.
  const poisoned = [], events = [];
  for (const message of messages) {
    let event = null;
    try { event = JSON.parse(message.value.toString()); } catch {}
    const errors = validateEvent(event);
    errors.length ? poisoned.push({ message, event, errors }) : events.push({ message, event });
  }

  // 2. One query: archive check for every event (a Kafka message whose events row is
  //    missing would fail the ledger's foreign key and stall the partition forever).
  const { rows: archived } = await pool.query(
    "SELECT event_id, tenant_id FROM events WHERE event_id = ANY($1::uuid[])",
    [events.map((e) => e.event.event_id)],
  );
  const archive = new Map(archived.map((r) => [r.event_id, r.tenant_id]));
  const valid = [];
  const traceCtx = new Map(); // event_id -> the ingest request's trace context (Kafka header)
  for (const e of events) {
    traceCtx.set(e.event.event_id, fromTraceparent(e.message.headers?.traceparent?.toString()));
    if (archive.get(e.event.event_id) === e.event.tenant_id) valid.push(e.event);
    else poisoned.push({ ...e, errors: ["event not found in the archive for this tenant"] });
  }
  await sendToDlq(poisoned, partition);

  // 3. One query: active endpoints of every tenant in the chunk.
  const tenants = [...new Set(valid.map((e) => e.tenant_id))];
  const { rows: endpoints } = await pool.query(
    `SELECT ep.tenant_id, ep.endpoint_id, ep.url, ep.secret_encrypted, ep.secret_previous_encrypted, ep.secret_rotated_at,
            t.retry_schedule_minutes
     FROM endpoints ep JOIN tenants t USING (tenant_id)
     WHERE ep.tenant_id = ANY($1) AND ep.is_active = TRUE`,
    [tenants],
  );
  if (!endpoints.length) return;
  const ids = endpoints.map((ep) => ep.endpoint_id);

  // 4. One query + one MGET: which endpoints already have something pending, and breakers.
  const { rows: pendingRows } = await pool.query(
    "SELECT DISTINCT endpoint_id FROM delivery_attempts WHERE endpoint_id = ANY($1::uuid[]) AND status = 'pending'",
    [ids],
  );
  const hasPending = new Set(pendingRows.map((r) => r.endpoint_id));
  const breakers = await getBreakers(ids).catch(() => new Map());

  // 5. Per endpoint, walk its events in Kafka order. Endpoints run concurrently - they
  //    have no ordering relationship to each other - but within one endpoint attempts
  //    are strictly sequential, and once something is queued, everything after it is
  //    queued behind it (per-endpoint order, ADR-008).
  const ledger = [], queue = [], outcomes = new Map(); // endpoint_id -> [{ ok, at }]
  await Promise.all(endpoints.map(async (endpoint) => {
    const breaker = breakers.get(endpoint.endpoint_id) ?? cb.initialState();
    // Already-known-bad endpoints get no inline attempt: let the worker handle them.
    let blocked = hasPending.has(endpoint.endpoint_id) || breaker.state !== "closed";
    const deadline = Date.now() + ENDPOINT_BUDGET_MS;

    for (const event of valid) {
      if (event.tenant_id !== endpoint.tenant_id) continue;
      // Out of time budget: hand the rest to the worker without a failed attempt, so a
      // slow-but-working endpoint can't hold the whole partition for N x timeout.
      if (blocked || Date.now() >= deadline) {
        blocked = true;
        queue.push({ event_id: event.event_id, endpoint_id: endpoint.endpoint_id, attempt_count: 0, delay_ms: 0, error: null, http_status: null });
        continue;
      }
      const started = Date.now();
      metrics.deliveryLatency.observe((started - Date.parse(event.created_at)) / 1000);
      // The attempt's span becomes part of this event's trace, not the chunk's.
      const result = await context.with(traceCtx.get(event.event_id), () => sendSigned(event, endpoint, INLINE_TIMEOUT_MS));
      metrics.attempts.inc({ source: "inline", outcome: outcomeLabel(result), tenant: event.tenant_id });
      metrics.attemptDuration.observe({ source: "inline" }, (Date.now() - started) / 1000);
      ledger.push({ event_id: event.event_id, endpoint_id: endpoint.endpoint_id, tenant_id: event.tenant_id,
        attempt_number: 1, ...result, latencyMs: Date.now() - started });
      if (!outcomes.has(endpoint.endpoint_id)) outcomes.set(endpoint.endpoint_id, []);
      outcomes.get(endpoint.endpoint_id).push({ ok: result.ok, at: Date.now() });
      await heartbeat();
      if (!result.ok) {
        blocked = true;
        // Timed out = maybe just slow: worker retries now with a longer timeout. Any
        // other failure is a real one and backs off per the retry policy.
        queue.push({ event_id: event.event_id, endpoint_id: endpoint.endpoint_id, attempt_count: 1,
          delay_ms: result.timedOut ? 0 : nextRetryDelay(1, retryPolicyFor(endpoint.retry_schedule_minutes)), error: result.error, http_status: result.httpStatus });
        log.info("inline attempt failed, queued for retry", {
          event_id: event.event_id, endpoint_id: endpoint.endpoint_id, tenant_id: event.tenant_id,
          error: result.error, http_status: result.httpStatus, retry_now: !!result.timedOut,
        });
      }
    }
  }));

  // 6. Record everything before the offset is committed (ADR-005). Queue rows go in one
  //    statement; ids are assigned in array order, which is what keeps per-endpoint
  //    order for rows sharing a created_at. ON CONFLICT: a redelivered chunk is a no-op.
  const links = [...new Set(ledger.map((l) => l.event_id))]
    .map((id) => trace.getSpanContext(traceCtx.get(id)))
    .filter((sc) => sc && trace.isSpanContextValid(sc))
    .map((sc) => ({ context: sc }));
  await recordAttempts(ledger, links);
  if (queue.length) {
    const col = (f) => queue.map(f);
    await pool.query(
      `INSERT INTO delivery_attempts (event_id, endpoint_id, attempt_count, next_retry_at, last_error, last_http_status)
       SELECT q.event_id, q.endpoint_id, q.attempt_count, NOW() + q.delay_ms * INTERVAL '1 millisecond', q.error, q.http_status
       FROM unnest($1::uuid[], $2::uuid[], $3::int[], $4::bigint[], $5::text[], $6::int[]) WITH ORDINALITY
            AS q(event_id, endpoint_id, attempt_count, delay_ms, error, http_status, n)
       ORDER BY q.n
       ON CONFLICT (event_id, endpoint_id) DO NOTHING`,
      [col((q) => q.event_id), col((q) => q.endpoint_id), col((q) => q.attempt_count), col((q) => q.delay_ms),
        col((q) => q.error), col((q) => q.http_status)],
    );
  }
  // Atomic per endpoint (compare-and-set), so outcomes reported at the same moment by
  // workers or other consumers aren't lost.
  await Promise.all([...outcomes].map(([id, list]) => applyOutcomes(id, list)))
    .catch((err) => log.error("breaker update failed", { err }));
  log.info("chunk processed", { partition, events: valid.length, delivered_inline: ledger.filter((l) => l.ok).length, queued: queue.length });
}

async function run() {
  initTracing("consumer");
  serveMetrics(9101);
  await ensureTopics();
  await dlqProducer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.events, fromBeginning: true });

  await consumer.run({
    partitionsConsumedConcurrently: EVENT_PARTITIONS, // capped by what this consumer is assigned
    eachBatchAutoResolve: false,
    // An offset is resolved only after its chunk's ledger and queue writes succeeded,
    // so the commit is gated on the record (spec §8, ADR-005). If a chunk throws, the
    // unresolved messages are redelivered - duplicates allowed, loss not.
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      for (let i = 0; i < batch.messages.length; i += CHUNK_SIZE) {
        if (!isRunning() || isStale()) return;
        const chunk = batch.messages.slice(i, i + CHUNK_SIZE);
        await processChunk(chunk, batch.partition, heartbeat);
        resolveOffset(chunk[chunk.length - 1].offset);
        await commitOffsetsIfNecessary();
        await heartbeat();
      }
      // offsetLag() = messages after this batch's last one: after processing, the real lag.
      metrics.consumerLag.set({ partition: String(batch.partition) }, Number(batch.offsetLag()));
    },
  });
}

// Graceful shutdown (spec failure scenario #10): stop taking new messages and let
// in-flight ones finish their ledger writes before exiting.
let stopping = false;
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, async () => {
    if (stopping) return;
    stopping = true;
    log.info("shutting down");
    await consumer.disconnect();
    await dlqProducer.disconnect();
    await closeRedis();
    await pool.end();
    process.exit(0);
  });
}

run().catch((err) => {
  log.error("consumer failed to start", { err });
  process.exit(1);
});
