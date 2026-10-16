import { pool, newProducer, closeRedis, redis, TOPICS, retryPolicyFor, FAST_MODE } from './lib/infra.js';
import { sendSigned, recordAttempts, pingEndpoint } from './lib/deliver.js';
import { getBreakers, applyOutcomes, reopensAt } from './lib/breaker.js';
import { initialState, current } from './core/circuit-breaker.js';
import { nextRetryDelay } from './core/backoff.js';
import { log } from './lib/log.js';
import { metrics, serveMetrics, client } from './lib/metrics.js';
import { initTracing, inSpan, fromTraceparent } from './lib/tracing.js';
import { onShutdown } from './lib/shutdown.js';
import { trace } from '@opentelemetry/api';

const POLL_MS = Number(process.env.WORKER_POLL_MS || 10_000);
const LEASE_SECONDS = 60;            // must exceed the largest timeout below + ledger write
const MAX_PER_TENANT = Number(process.env.MAX_PER_TENANT_PER_SWEEP || 10);
const MAX_PROCESSING_ERRORS = 3;     // poison-pill threshold (spec §9.11)
const PROBE_AFTER = 3;               // failed attempts before checking endpoint vs event (inline + 2 retries, ~6 min)
const PING_TIMEOUT_MS = 5000;        // largest attempt timeout (20s) + this + writes must stay under the lease
// Up to this many of an endpoint's queued rows per sweep, sent one after another. Sending
// stops starting new ones after BATCH_BUDGET_MS, so the last send (<= 20s timeout + 5s ping)
// still finishes inside the 60s lease every row of the batch was claimed with.
const BATCH_PER_ENDPOINT = Number(process.env.WORKER_BATCH_PER_ENDPOINT || 20);
const BATCH_BUDGET_MS = 10_000;
// Endpoint batches in flight per worker process: its fan-out to receivers.
const MAX_ENDPOINTS_IN_FLIGHT = Number(process.env.WORKER_MAX_ENDPOINTS || 200);

// Grows with each attempt: give a genuinely-slow-but-working endpoint more
// room on later tries, instead of judging it against the same fixed cutoff
// every time. Capped at the last value once attempts exceed the list length.
const TIMEOUT_SCHEDULE_MS = [5000, 8000, 12000, 15000, 20000];

function timeoutForAttempt(attemptCount) {
  const index = Math.min(attemptCount, TIMEOUT_SCHEDULE_MS.length - 1);
  return TIMEOUT_SCHEDULE_MS[index];
}

const dlqProducer = newProducer();

// System-wide state, read from Postgres/Redis when Prometheus scrapes. Served by the
// worker; if several workers run, give the extras METRICS_PORT=0 (or use max() in queries).
new client.Gauge({
  name: 'reliant_dlq_depth', help: 'Deliveries currently dead-lettered, by reason', labelNames: ['reason'],
  async collect() {
    const { rows } = await pool.query(`SELECT COALESCE(dead_reason, 'unknown') AS reason, count(*)::int AS n FROM delivery_attempts WHERE status = 'dead' GROUP BY 1`);
    this.reset();
    for (const r of rows) this.set({ reason: r.reason }, r.n);
  },
});
// Each gauge runs its own query: prom-client collects them concurrently, so one gauge
// reading another's result would be a scrape behind.
new client.Gauge({
  name: 'reliant_pending_deliveries', help: 'Deliveries waiting in the retry queue',
  async collect() {
    const { rows: [r] } = await pool.query(`SELECT count(*)::int AS n FROM delivery_attempts WHERE status = 'pending'`);
    this.set(r.n);
  },
});
new client.Gauge({
  name: 'reliant_oldest_pending_seconds', help: 'Age of the oldest pending delivery',
  async collect() {
    const { rows: [r] } = await pool.query(
      `SELECT COALESCE(EXTRACT(EPOCH FROM NOW() - min(created_at)), 0)::float AS age FROM delivery_attempts WHERE status = 'pending'`);
    this.set(r.age);
  },
});
// One series per currently-open breaker (1), so alerts can name the endpoint and tenant.
// Count them with count(reliant_circuit_breaker_open).
// ponytail: SCANs every breaker key per scrape; fine to ~100k endpoints, then keep a set of open ones.
new client.Gauge({
  name: 'reliant_circuit_breaker_open', help: 'Present (=1) for each endpoint whose circuit breaker is open',
  labelNames: ['endpoint_id', 'tenant'],
  async collect() {
    const r = await redis();
    const keys = [];
    for await (const k of r.scanIterator({ MATCH: 'cb:*', COUNT: 500 })) keys.push(k);
    const open = [];
    for (let i = 0; i < keys.length; i += 500) {
      const batch = keys.slice(i, i + 500);
      const raws = await r.mGet(batch);
      raws.forEach((raw, j) => { if (raw && current(JSON.parse(raw), Date.now()).state === 'open') open.push(batch[j].slice(3)); });
    }
    this.reset();
    if (!open.length) return;
    const { rows } = await pool.query('SELECT endpoint_id, tenant_id FROM endpoints WHERE endpoint_id::text = ANY($1)', [open]);
    for (const row of rows) this.set({ endpoint_id: row.endpoint_id, tenant: row.tenant_id }, 1);
  },
});

// Claims due work with a lease, so any number of worker processes can run at once.
//  - ranked: every pending row with its place in its endpoint's queue (pos 1 = oldest)
//    and whether it and every row before it is due and unlocked (free). The oldest row
//    counts whether due or not: skipping a row that's still backing off would let a newer
//    row overtake it - an ordering violation. So a backing-off or locked head takes its
//    whole endpoint out of this sweep.
//  - heads: endpoints whose head is free, at most `room` of them, and per tenant at most
//    MAX_PER_TENANT minus that tenant's batches already in flight here, so one tenant with
//    many failing endpoints can't take the whole worker.
//  - claimed: up to BATCH_PER_ENDPOINT rows from the front of each of those queues. The
//    UPDATE's locked_until check is re-evaluated under the row lock, so if two workers race
//    for the same row, exactly one gets it (sweep() then only uses a run starting at pos 1).
// ponytail: the per-tenant cap is per worker process, not global across workers.
async function claimDueDeliveries(room, busyByTenant) {
  const { rows } = await pool.query(
    `WITH ranked AS (
       SELECT da.id, da.endpoint_id, da.next_retry_at, ep.tenant_id,
              row_number() OVER w AS pos,
              bool_and(da.next_retry_at <= NOW() AND (da.locked_until IS NULL OR da.locked_until < NOW())) OVER w AS free
       FROM delivery_attempts da JOIN endpoints ep USING (endpoint_id)
       WHERE da.status = 'pending'
       WINDOW w AS (PARTITION BY da.endpoint_id ORDER BY da.created_at, da.id)
     ), heads AS (
       SELECT r.endpoint_id, r.next_retry_at,
              row_number() OVER (PARTITION BY r.tenant_id ORDER BY r.next_retry_at) + COALESCE(b.busy, 0) AS rn
       FROM ranked r LEFT JOIN unnest($5::text[], $6::int[]) AS b(tenant_id, busy) USING (tenant_id)
       WHERE r.pos = 1 AND r.free
     ), picked AS (
       SELECT endpoint_id FROM heads WHERE rn <= $2 ORDER BY next_retry_at LIMIT $4
     ), claimed AS (
       UPDATE delivery_attempts da SET locked_until = NOW() + ($1 * INTERVAL '1 second'), lease = gen_random_uuid()
       FROM ranked r JOIN picked USING (endpoint_id)
       WHERE da.id = r.id AND r.pos <= $3 AND r.free
         AND da.status = 'pending' AND (da.locked_until IS NULL OR da.locked_until < NOW())
       RETURNING da.*, r.pos
     )
     SELECT c.id, c.lease, c.pos, c.event_id, c.endpoint_id, c.attempt_count, c.processing_errors,
            e.tenant_id, e.type, e.payload, e.created_at, e.traceparent,
            ep.url, ep.secret_encrypted, ep.secret_previous_encrypted, ep.secret_rotated_at, t.retry_schedule_minutes
     FROM claimed c
     JOIN events e ON e.event_id = c.event_id
     JOIN endpoints ep ON ep.endpoint_id = c.endpoint_id
     JOIN tenants t ON t.tenant_id = e.tenant_id
     ORDER BY c.endpoint_id, c.pos`,
    [LEASE_SECONDS, MAX_PER_TENANT, BATCH_PER_ENDPOINT, room, [...busyByTenant.keys()], [...busyByTenant.values()]],
  );
  return rows;
}

// Hands claimed rows back untouched (fenced: only rows still carrying our lease).
async function release(attempts) {
  if (!attempts.length) return;
  await pool.query(
    `UPDATE delivery_attempts da SET locked_until = NULL
     FROM unnest($1::bigint[], $2::uuid[]) AS x(id, lease) WHERE da.id = x.id AND da.lease = x.lease`,
    [attempts.map((a) => a.id), attempts.map((a) => a.lease)]);
}

// Every write to a claimed row is fenced on its lease token ($1 = id, $2 = lease). If this
// worker stalled past LEASE_SECONDS (a long GC pause, a Postgres stall) and another worker
// re-claimed the row, our outcome is dropped instead of overwriting the new owner's state.
// It can't un-send our POST: that duplicate is what webhook-id is for. Our ledger row stays,
// because the attempt did happen.
async function updateLeased(attempt, set, params = []) {
  const { rowCount } = await pool.query(`UPDATE delivery_attempts SET ${set} WHERE id = $1 AND lease = $2`,
    [attempt.id, attempt.lease, ...params]);
  if (!rowCount) leaseLost(attempt);
  return rowCount > 0;
}

function leaseLost(attempt) {
  metrics.leaseLost.inc();
  log.warn('lease lost: another worker owns this row now, outcome not recorded on it',
    { event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, tenant_id: attempt.tenant_id });
}

// Dead-letters the claimed row (fenced on its lease). With `backlogReason`, every other
// pending row of the endpoint goes with it, in the same statement. That's for when the
// ENDPOINT is the problem: rows queued behind the head can't be delivered before it
// (ADR-008), and each would otherwise start its own full retry cycle against a dead
// endpoint, so K queued events took K x ~14.6h to clear. Now none waits more than one
// cycle. Replay-dead re-queues them in their original order once the endpoint is back.
async function deadLetter(attempt, reason, error, backlogReason = null) {
  const why = reason === 'endpoint_gone' ? 'got 410 Gone' : 'used up its retries';
  const { rows } = await pool.query(
    `WITH head AS (
       UPDATE delivery_attempts SET status = 'dead', dead_reason = $3, last_error = $4, terminal_at = NOW(), locked_until = NULL
       WHERE id = $1 AND lease = $2 RETURNING endpoint_id
     ), backlog AS (
       UPDATE delivery_attempts da
       SET status = 'dead', dead_reason = $5, last_error = $6, terminal_at = NOW(), locked_until = NULL
       FROM head
       WHERE $5::text IS NOT NULL AND da.endpoint_id = head.endpoint_id AND da.status = 'pending' AND da.id <> $1
         AND (da.locked_until IS NULL OR da.locked_until < NOW())
       RETURNING da.event_id
     )
     SELECT $7::uuid AS event_id FROM head
     UNION ALL SELECT event_id FROM backlog`,
    [attempt.id, attempt.lease, reason, error, backlogReason, `queued behind ${attempt.event_id}, which ${why}`, attempt.event_id],
  );
  if (!rows.length) return leaseLost(attempt);
  const backlog = rows.length - 1;
  // Postgres is the source of truth; the topic is just a fan-out for alerting tools.
  await dlqProducer.send({
    topic: TOPICS.dlq,
    messages: rows.map((r, i) => ({ key: attempt.tenant_id, value: JSON.stringify({
      reason: i ? backlogReason : reason, event_id: r.event_id, endpoint_id: attempt.endpoint_id,
      tenant_id: attempt.tenant_id, last_error: i ? `queued behind ${attempt.event_id}, which ${why}` : error,
    }) })),
  }).catch((err) => log.error('DLQ topic publish failed (rows are still dead in Postgres)', { event_id: attempt.event_id, err }));
  metrics.deadLetters.inc({ reason });
  if (backlog) metrics.deadLetters.inc({ reason: backlogReason }, backlog);
  trace.getActiveSpan()?.addEvent('dead_lettered', { reason, backlog });
  log.warn('dead-lettered', { reason, event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, tenant_id: attempt.tenant_id,
    error, ...(backlog && { backlog_dead_lettered: backlog }) });
}

// 410 Gone is the receiver saying "stop sending here, for good" (the Standard Webhooks
// convention). It's the only status trusted that far: anything else could be a deploy or a
// bug (ADR-008). An owner re-enables the endpoint and replays its dead letters.
async function disableEndpoint(attempt, reason) {
  const { rowCount } = await pool.query(
    'UPDATE endpoints SET is_active = FALSE, disabled_reason = $2, disabled_at = NOW() WHERE endpoint_id = $1 AND is_active',
    [attempt.endpoint_id, reason]);
  if (!rowCount) return;
  metrics.endpointsDisabled.inc({ reason });
  log.warn('endpoint disabled', { endpoint_id: attempt.endpoint_id, tenant_id: attempt.tenant_id, reason });
}

// One send. A success is only collected into `delivered` (recordDelivered writes the whole
// sweep's successes at once); a failure is recorded and handled here, after `beforeFailure`
// (which hands the rest of the endpoint's batch back, so a dead-letter of the backlog sees it).
// Returns true on success.
async function processOneDelivery(attempt, delivered, beforeFailure) {
  const attemptNumber = attempt.attempt_count + 1;
  const timeoutMs = timeoutForAttempt(attempt.attempt_count);
  const event = { event_id: attempt.event_id, tenant_id: attempt.tenant_id, type: attempt.type, payload: attempt.payload, created_at: attempt.created_at };
  const started = Date.now();
  if (attemptNumber === 1) metrics.deliveryLatency.observe((started - new Date(attempt.created_at).getTime()) / 1000); // queued without an inline try
  const result = await sendSigned(event, attempt, timeoutMs);
  const latencyMs = Date.now() - started;
  metrics.attempts.inc({ source: 'retry', outcome: result.ok ? 'delivered' : result.timedOut ? 'timeout' : 'failed', tenant: attempt.tenant_id });
  metrics.attemptDuration.observe({ source: 'retry' }, latencyMs / 1000);
  metrics.attemptNumber.observe(attemptNumber);

  if (result.ok) {
    delivered.push({ attempt, attemptNumber, result, latencyMs, at: Date.now() });
    return true;
  }
  await beforeFailure();
  await recordAttempts([{ event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, tenant_id: attempt.tenant_id,
    attempt_number: attemptNumber, ...result, latencyMs }]);
  await applyOutcomes(attempt.endpoint_id, [{ ok: false, at: Date.now() }]).catch((err) => log.error('breaker update failed', { endpoint_id: attempt.endpoint_id, err }));

  if (result.httpStatus === 410) {
    if (!await updateLeased(attempt, `attempt_count = $3, last_http_status = $4`, [attemptNumber, 410])) return;
    await disableEndpoint(attempt, 'http_410');
    await deadLetter(attempt, 'endpoint_gone', `${result.error}: endpoint disabled`, 'endpoint_gone');
    return;
  }

  // Endpoint problem or event problem? From PROBE_AFTER failures on, and before giving up,
  // ping the endpoint. Healthy ping = the endpoint works but keeps refusing THIS event:
  // set it aside so it stops blocking every later event for this endpoint (ADR-008).
  // Failed ping = the endpoint is down: keep this event at the head and keep backing off.
  // Not on a 429: that's the endpoint throttling us, and a ping it happens to let through
  // would wrongly blame the event.
  const scheduled = nextRetryDelay(attemptNumber, retryPolicyFor(attempt.retry_schedule_minutes));
  let endpointHealthy = false;
  if ((attemptNumber >= PROBE_AFTER || scheduled === null) && result.httpStatus !== 429) {
    endpointHealthy = (await pingEndpoint(attempt, attempt.tenant_id, PING_TIMEOUT_MS)).ok;
    metrics.pings.inc({ result: endpointHealthy ? 'healthy' : 'failed' });
    log.info('endpoint probed', { event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, attempt: attemptNumber,
      verdict: endpointHealthy ? 'event is the problem' : 'endpoint is the problem' });
  }

  if (endpointHealthy) {
    if (!await updateLeased(attempt, `attempt_count = $3, last_http_status = $4`, [attemptNumber, result.httpStatus])) return;
    await deadLetter(attempt, 'payload_rejected', `${result.error} (endpoint answered a ping fine, so this event is the problem)`);
    return;
  }
  if (scheduled === null) { // out of retries and the endpoint itself is failing: its backlog goes too
    if (!await updateLeased(attempt, `attempt_count = $3, last_http_status = $4`, [attemptNumber, result.httpStatus])) return;
    await deadLetter(attempt, 'delivery_exhausted', result.error, 'endpoint_unreachable');
    return;
  }
  // The receiver's own Retry-After (429/503) wins over the schedule. The attempt still
  // counts, so an endpoint that throttles forever still ends in the DLQ.
  const delayMs = result.retryAfterMs ?? scheduled;
  if (!await updateLeased(attempt,
    `attempt_count = $3, next_retry_at = NOW() + ($4 * INTERVAL '1 millisecond'), last_error = $5, last_http_status = $6, locked_until = NULL`,
    [attemptNumber, delayMs, result.error, result.httpStatus])) return;
  log.info('still failing', { event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, attempt: attemptNumber,
    error: result.error, next_retry_in_s: Math.round(delayMs / 1000) });
  return false;
}

// The sweep's successes in three writes, however many there are: one ledger INSERT, the
// breakers, and one UPDATE fenced on every row's lease. Was two Postgres writes per row:
// 100 rows a sweep queued for the pool's 10 connections, and a backlogged endpoint drained
// at 4.5 rows/s (loadtest retry-drain), slower than the 10/s it was receiving in the
// rolling-restart drill, so it never caught up. The ledger goes first, as before: a crash
// between the two re-sends (a duplicate), never loses the record.
async function recordDelivered(delivered) {
  if (!delivered.length) return;
  await recordAttempts(delivered.map((d) => ({ event_id: d.attempt.event_id, endpoint_id: d.attempt.endpoint_id,
    tenant_id: d.attempt.tenant_id, attempt_number: d.attemptNumber, ...d.result, latencyMs: d.latencyMs })));
  const byEndpoint = Map.groupBy(delivered, (d) => d.attempt.endpoint_id);
  await Promise.all([...byEndpoint].map(([id, list]) => applyOutcomes(id, list.map((d) => ({ ok: true, at: d.at })))
    .catch((err) => log.error('breaker update failed', { endpoint_id: id, err }))));
  const col = (f) => delivered.map(f);
  const { rows } = await pool.query(
    `UPDATE delivery_attempts da SET status = 'delivered', attempt_count = x.n, last_http_status = x.code,
       last_error = NULL, terminal_at = NOW(), locked_until = NULL
     FROM unnest($1::bigint[], $2::uuid[], $3::int[], $4::int[]) AS x(id, lease, n, code)
     WHERE da.id = x.id AND da.lease = x.lease RETURNING da.id`,
    [col((d) => d.attempt.id), col((d) => d.attempt.lease), col((d) => d.attemptNumber), col((d) => d.result.httpStatus)]);
  const updated = new Set(rows.map((r) => String(r.id)));
  for (const d of delivered) {
    if (!updated.has(String(d.attempt.id))) { leaseLost(d.attempt); continue; }
    log.info('delivered', { event_id: d.attempt.event_id, endpoint_id: d.attempt.endpoint_id, tenant_id: d.attempt.tenant_id,
      attempt: d.attemptNumber, http_status: d.result.httpStatus });
  }
}

// An unexpected throw (not an HTTP failure - those are handled above) means something
// about this row/system is broken. Retry a few times, then park it as a processing
// error so it can't crash-loop the worker forever.
async function processSafely(attempt, delivered, beforeFailure) {
  try {
    // Continues the event's original trace (events.traceparent), even hours after ingest.
    return await inSpan('deliver.retry', {
      parent: fromTraceparent(attempt.traceparent),
      attributes: { 'reliant.event_id': attempt.event_id, 'reliant.endpoint_id': attempt.endpoint_id, 'reliant.attempt': attempt.attempt_count + 1 },
    }, () => processOneDelivery(attempt, delivered, beforeFailure));
  } catch (err) {
    await beforeFailure().catch(() => {});
    log.error('processing error', { event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, err });
    const errors = attempt.processing_errors + 1;
    const owned = await updateLeased(attempt, `processing_errors = $3, locked_until = NULL`, [errors])
      .catch(() => false); // if even this fails, the lease expires and the row is retried
    if (owned && errors >= MAX_PROCESSING_ERRORS) await deadLetter(attempt, 'processing_error', err.message).catch(() => {});
    return false;
  }
}

// One endpoint's claimed rows, oldest first, strictly one request at a time: the next is
// sent only after the previous one succeeded. Order and the thundering-herd guarantee hold
// as with one row per sweep; a recovering endpoint (half-open breaker) just gets its
// backlog at one request at a time instead of one per sweep.
async function processEndpoint(rows, breaker, delivered) {
  // Only a run starting at the endpoint's oldest row is ours to send: if a racing worker took
  // the head, the rest must wait behind it.
  let run = 0;
  while (run < rows.length && Number(rows[run].pos) === run + 1) run++; // bigint: pg returns a string
  await release(rows.slice(run));
  if (!run) return;
  // Breaker open: endpoint is known-down. Push the head to when a trial is allowed, without
  // spending an attempt; the rows behind it wait anyway.
  if (breaker.state === 'open') {
    await release(rows.slice(1, run));
    await updateLeased(rows[0], `next_retry_at = $3, locked_until = NULL`, [reopensAt(breaker)]);
    log.info('circuit open, deferred', { event_id: rows[0].event_id, endpoint_id: rows[0].endpoint_id, until: reopensAt(breaker) });
    return;
  }
  const deadline = Date.now() + BATCH_BUDGET_MS;
  for (let i = 0; i < run; i++) {
    let released = false;
    const releaseRest = async () => { if (!released) { released = true; await release(rows.slice(i + 1, run)); } };
    if (stopping || (i > 0 && Date.now() >= deadline)) return release(rows.slice(i, run));
    if (!await processSafely(rows[i], delivered, releaseRest)) return releaseRest();
  }
}

let stopping = false;
const inFlight = new Set();      // running endpoint batches
const busyByTenant = new Map();  // tenant -> its batches in flight here
let wake = () => {};             // ends the idle wait early (a batch finished, or shutdown)

// One endpoint's batch: send, then record its own successes. Endpoints have no ordering
// relationship to each other, so batches run side by side and none waits for another:
// a slow endpoint holds only itself (before, every sweep waited for its slowest endpoint).
function startBatch(rows, breaker) {
  const tenant = rows[0].tenant_id;
  busyByTenant.set(tenant, (busyByTenant.get(tenant) || 0) + 1);
  const delivered = [];
  const p = processEndpoint(rows, breaker, delivered)
    .catch((err) => log.error('endpoint batch failed', { endpoint_id: rows[0].endpoint_id, err }))
    .then(() => recordDelivered(delivered))
    .catch((err) => log.error('recording deliveries failed; their leases expire and they are re-sent', { count: delivered.length, err }))
    .finally(() => {
      inFlight.delete(p);
      const n = busyByTenant.get(tenant) - 1;
      n ? busyByTenant.set(tenant, n) : busyByTenant.delete(tenant);
      wake();
    });
  inFlight.add(p);
}

// Claims as many endpoints as there's room for and starts their batches. Returns how many.
async function claimMore() {
  const room = MAX_ENDPOINTS_IN_FLIGHT - inFlight.size;
  if (room <= 0) return 0;
  const rows = await claimDueDeliveries(room, busyByTenant);
  if (!rows.length) return 0;
  const byEndpoint = Map.groupBy(rows, (r) => r.endpoint_id);
  log.info('claimed due deliveries', { count: rows.length, endpoints: byEndpoint.size, in_flight: inFlight.size });
  const breakers = await getBreakers([...byEndpoint.keys()]).catch(() => new Map());
  for (const [id, list] of byEndpoint) startBatch(list, breakers.get(id) ?? initialState());
  return byEndpoint.size;
}

// Claim whenever there's room and work. With nothing new to claim, wait until a batch ends
// (its endpoint may have more queued) or POLL_MS passes. Never sleeping while work is due
// matters: waiting POLL_MS between claims once capped a backlogged endpoint at one
// delivery per 10s (incident 002).
async function pump() {
  while (!stopping) {
    const started = await claimMore().catch((err) => { log.error('claim failed', { err }); return 0; });
    if (started && inFlight.size < MAX_ENDPOINTS_IN_FLIGHT) continue;
    await new Promise((resolve) => { wake = resolve; setTimeout(resolve, POLL_MS).unref(); });
  }
}

async function run() {
  // Registered FIRST: a SIGTERM that lands during startup or the first sweep must not be
  // ignored (Node as PID 1 drops unhandled SIGTERM - found by a mid-delivery shutdown test).
  let pumping = Promise.resolve();
  onShutdown(async () => {
    stopping = true; // stop claiming new work (each batch stops sending and hands its rest back)...
    wake();
    await pumping;
    while (inFlight.size) await Promise.all(inFlight); // ...and let in-flight deliveries and their ledger writes finish
    await dlqProducer.disconnect();
    await closeRedis();
    await pool.end();
  });
  initTracing('worker');
  serveMetrics(9102);
  await dlqProducer.connect();
  log.info('delivery worker started', { poll_ms: POLL_MS, fast_mode: FAST_MODE });

  pumping = pump();
  await pumping;
}

run().catch((err) => {
  log.error('delivery worker failed to start', { err });
  process.exit(1);
});
