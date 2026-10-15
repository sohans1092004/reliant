import { pool, kafka, newProducer, ensureTopics, closeRedis, TOPICS, retryPolicy } from "./lib/infra.js";
import { attemptDelivery } from "./lib/deliver.js";
import { getBreaker } from "./lib/breaker.js";
import { initialState } from "./core/circuit-breaker.js";
import { nextRetryDelay } from "./core/backoff.js";
import { validateEvent } from "./core/event.js";

const INLINE_TIMEOUT_MS = 3000;

const consumer = kafka.consumer({ groupId: "delivery-workers" });
const dlqProducer = newProducer();

// Queue a delivery for delivery-worker.js. ON CONFLICT: if Kafka redelivers a message
// we already handled (crash before offset commit), the existing row wins.
async function queueDelivery(eventId, endpointId, { attemptCount = 0, delayMs = 0, error = null, httpStatus = null } = {}) {
  await pool.query(
    `INSERT INTO delivery_attempts (event_id, endpoint_id, attempt_count, next_retry_at, last_error, last_http_status)
     VALUES ($1, $2, $3, NOW() + ($4 * INTERVAL '1 millisecond'), $5, $6)
     ON CONFLICT (event_id, endpoint_id) DO NOTHING`,
    [eventId, endpointId, attemptCount, delayMs, error, httpStatus],
  );
}

async function deliverToEndpoint(event, endpoint) {
  // If an earlier delivery to this SAME endpoint is still unresolved,
  // don't race ahead of it - just queue this one behind it.
  const { rows: pendingAhead } = await pool.query(
    `SELECT 1 FROM delivery_attempts WHERE endpoint_id = $1 AND status = 'pending' LIMIT 1`,
    [endpoint.endpoint_id],
  );
  if (pendingAhead.length > 0) {
    await queueDelivery(event.event_id, endpoint.endpoint_id);
    console.log("Endpoint has a delivery already pending, queued behind it:", endpoint.url);
    return;
  }

  // Breaker not closed = endpoint known to be failing. Don't spend 3s finding that out
  // again; let the worker handle it (it respects the breaker's cooldown).
  const breaker = await getBreaker(endpoint.endpoint_id).catch(() => initialState());
  if (breaker.state !== "closed") {
    await queueDelivery(event.event_id, endpoint.endpoint_id);
    console.log(`Circuit ${breaker.state} for ${endpoint.url}, queued without trying`);
    return;
  }

  // Nothing ahead of us - safe to try immediately, but bound the wait.
  const result = await attemptDelivery(event, endpoint, 1, INLINE_TIMEOUT_MS);
  if (result.ok) {
    console.log("Delivered to", endpoint.url, "- status:", result.httpStatus);
    return;
  }
  await queueDelivery(event.event_id, endpoint.endpoint_id, {
    attemptCount: 1,
    delayMs: nextRetryDelay(1, retryPolicy),
    error: result.error,
    httpStatus: result.httpStatus,
  });
  console.log("Inline attempt failed/timed out, queued for retry:", endpoint.url, "-", result.error);
}

async function run() {
  await ensureTopics();
  await dlqProducer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.events, fromBeginning: true });

  await consumer.run({
    partitionsConsumedConcurrently: 3, // must be <= the topic's actual partition count
    // kafkajs commits the offset only after this resolves, so the offset commit is
    // gated on the ledger / delivery_attempts writes below (spec §8, ADR-005).
    eachMessage: async ({ message, partition }) => {
      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch {
        event = null;
      }
      const problems = validateEvent(event);
      if (problems.length > 0) {
        // Poison pill: can never be processed, retrying would only crash-loop.
        // Park it on the DLQ topic and move on. (There's no events row to point at.)
        await dlqProducer.send({
          topic: TOPICS.dlq,
          messages: [{ key: event?.tenant_id ?? null, value: JSON.stringify({
            reason: "processing_error", errors: problems, partition, offset: message.offset,
            raw: message.value.toString().slice(0, 10_000),
          }) }],
        });
        console.log(`Malformed message at partition ${partition} offset ${message.offset} sent to DLQ:`, problems.join("; "));
        return;
      }
      console.log("Received event:", event.event_id, "for", event.tenant_id);

      // Look up this tenant's registered endpoint(s) instead of using a fixed URL.
      const { rows } = await pool.query(
        `SELECT endpoint_id, url, secret_encrypted, secret_previous_encrypted, secret_rotated_at
         FROM endpoints WHERE tenant_id = $1 AND is_active = TRUE`,
        [event.tenant_id],
      );

      // Different endpoints have no ordering relationship to each other, so one
      // slow endpoint's 3s timeout doesn't delay its tenant-mates on the same event.
      await Promise.all(rows.map((endpoint) => deliverToEndpoint(event, endpoint)));
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
