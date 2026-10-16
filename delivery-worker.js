import { pool, newProducer, closeRedis, redis, TOPICS, retryPolicyFor, FAST_MODE } from './lib/infra.js';
import { attemptDelivery, pingEndpoint } from './lib/deliver.js';
import { getBreaker, reopensAt } from './lib/breaker.js';
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
//  - heads: each endpoint's single OLDEST pending row, due or not. Filtering on
//    next_retry_at before DISTINCT ON would let a newer, due row overtake an older
//    row that's still backing off - an ordering violation.
//  - due: heads whose time has come, at most MAX_PER_TENANT per tenant per sweep, so
//    one tenant with many failing endpoints can't take the whole worker.
//  - The UPDATE's locked_until check is re-evaluated under the row lock, so if two
//    workers race for the same row, exactly one gets it.
// ponytail: the per-tenant cap is per worker per sweep, not global across workers.
async function claimDueDeliveries() {
  const { rows } = await pool.query(
    `WITH heads AS (
       SELECT DISTINCT ON (da.endpoint_id) da.id, da.next_retry_at, ep.tenant_id
       FROM delivery_attempts da JOIN endpoints ep USING (endpoint_id)
       WHERE da.status = 'pending'
       ORDER BY da.endpoint_id, da.created_at, da.id
     ), due AS (
       SELECT id, row_number() OVER (PARTITION BY tenant_id ORDER BY next_retry_at) AS rn
       FROM heads WHERE next_retry_at <= NOW()
     ), claimed AS (
       UPDATE delivery_attempts da SET locked_until = NOW() + ($1 * INTERVAL '1 second'), lease = gen_random_uuid()
       FROM due
       WHERE da.id = due.id AND due.rn <= $2
         AND da.status = 'pending' AND (da.locked_until IS NULL OR da.locked_until < NOW())
       RETURNING da.*
     )
     SELECT c.id, c.lease, c.event_id, c.endpoint_id, c.attempt_count, c.processing_errors,
            e.tenant_id, e.type, e.payload, e.created_at, e.traceparent,
            ep.url, ep.secret_encrypted, ep.secret_previous_encrypted, ep.secret_rotated_at, t.retry_schedule_minutes
     FROM claimed c
     JOIN events e ON e.event_id = c.event_id
     JOIN endpoints ep ON ep.endpoint_id = c.endpoint_id
     JOIN tenants t ON t.tenant_id = e.tenant_id`,
    [LEASE_SECONDS, MAX_PER_TENANT],
  );
  return rows;
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

async function processOneDelivery(attempt) {
  // Breaker open: endpoint is known-down. Push the row to when a trial is allowed,
  // without spending an attempt. Half-open: this is the trial - one per endpoint per
  // sweep (DISTINCT ON), which is what stops a thundering herd on recovery.
  const breaker = await getBreaker(attempt.endpoint_id).catch(() => initialState());
  if (breaker.state === 'open') {
    await updateLeased(attempt, `next_retry_at = $3, locked_until = NULL`, [reopensAt(breaker)]);
    log.info('circuit open, deferred', { event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, until: reopensAt(breaker) });
    return;
  }

  const attemptNumber = attempt.attempt_count + 1;
  const timeoutMs = timeoutForAttempt(attempt.attempt_count);
  const event = { event_id: attempt.event_id, tenant_id: attempt.tenant_id, type: attempt.type, payload: attempt.payload, created_at: attempt.created_at };
  const started = Date.now();
  if (attemptNumber === 1) metrics.deliveryLatency.observe((started - new Date(attempt.created_at).getTime()) / 1000); // queued without an inline try
  const result = await attemptDelivery(event, attempt, attemptNumber, timeoutMs);
  metrics.attempts.inc({ source: 'retry', outcome: result.ok ? 'delivered' : result.timedOut ? 'timeout' : 'failed', tenant: attempt.tenant_id });
  metrics.attemptDuration.observe({ source: 'retry' }, (Date.now() - started) / 1000);
  metrics.attemptNumber.observe(attemptNumber);

  if (result.ok) {
    if (!await updateLeased(attempt,
      `status = 'delivered', attempt_count = $3, last_http_status = $4, last_error = NULL, terminal_at = NOW(), locked_until = NULL`,
      [attemptNumber, result.httpStatus])) return;
    log.info('delivered', { event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, tenant_id: attempt.tenant_id, attempt: attemptNumber, http_status: result.httpStatus });
    return;
  }

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
}

// An unexpected throw (not an HTTP failure - those are handled above) means something
// about this row/system is broken. Retry a few times, then park it as a processing
// error so it can't crash-loop the worker forever.
async function processSafely(attempt) {
  try {
    // Continues the event's original trace (events.traceparent), even hours after ingest.
    await inSpan('deliver.retry', {
      parent: fromTraceparent(attempt.traceparent),
      attributes: { 'reliant.event_id': attempt.event_id, 'reliant.endpoint_id': attempt.endpoint_id, 'reliant.attempt': attempt.attempt_count + 1 },
    }, () => processOneDelivery(attempt));
  } catch (err) {
    log.error('processing error', { event_id: attempt.event_id, endpoint_id: attempt.endpoint_id, err });
    const errors = attempt.processing_errors + 1;
    const owned = await updateLeased(attempt, `processing_errors = $3, locked_until = NULL`, [errors])
      .catch(() => false); // if even this fails, the lease expires and the row is retried
    if (owned && errors >= MAX_PROCESSING_ERRORS) await deadLetter(attempt, 'processing_error', err.message).catch(() => {});
  }
}

let stopping = false;
let currentSweep = Promise.resolve();

// Returns how many rows it claimed: while there's due work, the next sweep starts right away.
async function sweep() {
  const rows = await claimDueDeliveries();
  if (rows.length === 0) return 0;
  log.info('claimed due deliveries', { count: rows.length });
  // Different rows here are for DIFFERENT endpoints (the query guarantees that),
  // so they have no ordering relationship to each other - safe to run at once.
  await Promise.all(rows.map(processSafely));
  return rows.length;
}

async function run() {
  // Registered FIRST: a SIGTERM that lands during startup or the first sweep must not be
  // ignored (Node as PID 1 drops unhandled SIGTERM - found by a mid-delivery shutdown test).
  let timer;
  onShutdown(async () => {
    stopping = true; // stop claiming new work...
    clearInterval(timer);
    await currentSweep; // ...and let in-flight deliveries and their ledger writes finish
    await dlqProducer.disconnect();
    await closeRedis();
    await pool.end();
  });
  initTracing('worker');
  serveMetrics(9102);
  await dlqProducer.connect();
  log.info('delivery worker started', { poll_ms: POLL_MS, fast_mode: FAST_MODE });

  // Leases make overlapping sweeps safe, but skipping keeps one process from piling up.
  let isRunning = false;
  async function tick() {
    if (isRunning || stopping) return;
    isRunning = true;
    // Keep sweeping while there's due work. Each sweep takes ONE row per endpoint (ordering),
    // so waiting POLL_MS between sweeps capped a backlogged endpoint at one delivery per
    // 10s: 73 queued events took 12 minutes after the Redis drill (incident 002).
    let claimed;
    do {
      currentSweep = sweep().catch((err) => { log.error('sweep failed', { err }); return 0; });
      claimed = await currentSweep;
    } while (claimed > 0 && !stopping);
    isRunning = false;
  }

  timer = setInterval(tick, POLL_MS);
  await tick();
}

run().catch((err) => {
  log.error('delivery worker failed to start', { err });
  process.exit(1);
});
