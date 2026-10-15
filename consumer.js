import { pool, kafka, newProducer, ensureTopics, closeRedis, TOPICS, retryPolicy } from "./lib/infra.js";
import { sendSigned, recordAttempts } from "./lib/deliver.js";
import { getBreakers, saveBreakers } from "./lib/breaker.js";
import * as cb from "./core/circuit-breaker.js";
import { nextRetryDelay } from "./core/backoff.js";
import { validateEvent } from "./core/event.js";

const INLINE_TIMEOUT_MS = 3000;   // one inline attempt never waits longer than this
const ENDPOINT_BUDGET_MS = 3000;  // per endpoint per chunk: stop STARTING inline attempts after this
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
  for (const p of poisoned) console.log(`Unprocessable message at partition ${partition} offset ${p.message.offset} sent to DLQ:`, p.errors.join("; "));
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
  for (const e of events) {
    if (archive.get(e.event.event_id) === e.event.tenant_id) valid.push(e.event);
    else poisoned.push({ ...e, errors: ["event not found in the archive for this tenant"] });
  }
  await sendToDlq(poisoned, partition);

  // 3. One query: active endpoints of every tenant in the chunk.
  const tenants = [...new Set(valid.map((e) => e.tenant_id))];
  const { rows: endpoints } = await pool.query(
    `SELECT tenant_id, endpoint_id, url, secret_encrypted, secret_previous_encrypted, secret_rotated_at
     FROM endpoints WHERE tenant_id = ANY($1) AND is_active = TRUE`,
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
  const ledger = [], queue = [], breakerChanges = new Map();
  await Promise.all(endpoints.map(async (endpoint) => {
    const before = breakers.get(endpoint.endpoint_id) ?? cb.initialState();
    let breaker = before;
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
      const result = await sendSigned(event, endpoint, INLINE_TIMEOUT_MS);
      ledger.push({ event_id: event.event_id, endpoint_id: endpoint.endpoint_id, tenant_id: event.tenant_id,
        attempt_number: 1, ...result, latencyMs: Date.now() - started });
      breaker = cb.record(breaker, result.ok, Date.now());
      await heartbeat();
      if (!result.ok) {
        blocked = true;
        queue.push({ event_id: event.event_id, endpoint_id: endpoint.endpoint_id, attempt_count: 1,
          delay_ms: nextRetryDelay(1, retryPolicy), error: result.error, http_status: result.httpStatus });
        console.log("Inline attempt failed/timed out, queued for retry:", endpoint.url, "-", result.error);
      }
    }
    if (JSON.stringify(breaker) !== JSON.stringify(before)) breakerChanges.set(endpoint.endpoint_id, { before, after: breaker });
  }));

  // 6. Record everything before the offset is committed (ADR-005). Queue rows go in one
  //    statement; ids are assigned in array order, which is what keeps per-endpoint
  //    order for rows sharing a created_at. ON CONFLICT: a redelivered chunk is a no-op.
  await recordAttempts(ledger);
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
  await saveBreakers(breakerChanges).catch((err) => console.error("Breaker update failed:", err.message));
  console.log(`Partition ${partition}: ${valid.length} events, ${ledger.filter((l) => l.ok).length} delivered inline, ${queue.length} queued`);
}

async function run() {
  await ensureTopics();
  await dlqProducer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.events, fromBeginning: true });

  await consumer.run({
    partitionsConsumedConcurrently: 3, // must be <= the topic's actual partition count
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
    console.log("Shutting down consumer...");
    await consumer.disconnect();
    await dlqProducer.disconnect();
    await closeRedis();
    await pool.end();
    process.exit(0);
  });
}

run().catch((err) => {
  console.error("Consumer failed to start:", err);
  process.exit(1);
});
