import { pool, newProducer, closeRedis, TOPICS, retryPolicyFor, FAST_MODE } from './lib/infra.js';
import { attemptDelivery, pingEndpoint } from './lib/deliver.js';
import { getBreaker, reopensAt } from './lib/breaker.js';
import { initialState } from './core/circuit-breaker.js';
import { nextRetryDelay } from './core/backoff.js';

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
       UPDATE delivery_attempts da SET locked_until = NOW() + ($1 * INTERVAL '1 second')
       FROM due
       WHERE da.id = due.id AND due.rn <= $2
         AND da.status = 'pending' AND (da.locked_until IS NULL OR da.locked_until < NOW())
       RETURNING da.*
     )
     SELECT c.id, c.event_id, c.endpoint_id, c.attempt_count, c.processing_errors,
            e.tenant_id, e.type, e.payload, e.created_at,
            ep.url, ep.secret_encrypted, ep.secret_previous_encrypted, ep.secret_rotated_at, t.retry_schedule_minutes
     FROM claimed c
     JOIN events e ON e.event_id = c.event_id
     JOIN endpoints ep ON ep.endpoint_id = c.endpoint_id
     JOIN tenants t ON t.tenant_id = e.tenant_id`,
    [LEASE_SECONDS, MAX_PER_TENANT],
  );
  return rows;
}

async function deadLetter(attempt, reason, error) {
  await pool.query(
    `UPDATE delivery_attempts
     SET status = 'dead', dead_reason = $2, last_error = $3, terminal_at = NOW(), locked_until = NULL
     WHERE id = $1`,
    [attempt.id, reason, error],
  );
  // Postgres is the source of truth; the topic is just a fan-out for alerting tools.
  await dlqProducer.send({
    topic: TOPICS.dlq,
    messages: [{ key: attempt.tenant_id, value: JSON.stringify({
      reason, event_id: attempt.event_id, endpoint_id: attempt.endpoint_id,
      tenant_id: attempt.tenant_id, last_error: error,
    }) }],
  }).catch((err) => console.error('DLQ topic publish failed (row is still dead in Postgres):', err.message));
  console.log(`Dead-lettered (${reason}):`, attempt.event_id, '->', attempt.url);
}

async function processOneDelivery(attempt) {
  // Breaker open: endpoint is known-down. Push the row to when a trial is allowed,
  // without spending an attempt. Half-open: this is the trial - one per endpoint per
  // sweep (DISTINCT ON), which is what stops a thundering herd on recovery.
  const breaker = await getBreaker(attempt.endpoint_id).catch(() => initialState());
  if (breaker.state === 'open') {
    await pool.query(
      `UPDATE delivery_attempts SET next_retry_at = $2, locked_until = NULL WHERE id = $1`,
      [attempt.id, reopensAt(breaker)],
    );
    console.log('Circuit open, deferred:', attempt.event_id, '->', attempt.url);
    return;
  }

  const attemptNumber = attempt.attempt_count + 1;
  const timeoutMs = timeoutForAttempt(attempt.attempt_count);
  const event = { event_id: attempt.event_id, tenant_id: attempt.tenant_id, type: attempt.type, payload: attempt.payload, created_at: attempt.created_at };
  const result = await attemptDelivery(event, attempt, attemptNumber, timeoutMs);

  if (result.ok) {
    await pool.query(
      `UPDATE delivery_attempts
       SET status = 'delivered', attempt_count = $2, last_http_status = $3, last_error = NULL, terminal_at = NOW(), locked_until = NULL
       WHERE id = $1`,
      [attempt.id, attemptNumber, result.httpStatus],
    );
    console.log('Delivered (queued):', attempt.event_id, '->', attempt.url, '-', result.httpStatus);
    return;
  }

  // Endpoint problem or event problem? From PROBE_AFTER failures on, ping the endpoint.
  // Healthy ping = the endpoint works but keeps refusing THIS event: set it aside so it
  // stops blocking every later event for this endpoint (ADR-008). Failed ping = the
  // endpoint is down: keep this event at the head and keep backing off.
  let endpointHealthy = false;
  if (attemptNumber >= PROBE_AFTER) {
    endpointHealthy = (await pingEndpoint(attempt, attempt.tenant_id, PING_TIMEOUT_MS)).ok;
    console.log(`Probe after attempt ${attemptNumber}: endpoint ${endpointHealthy ? 'healthy -> event is the problem' : 'also failing -> endpoint is the problem'}:`, attempt.url);
  }

  const delayMs = nextRetryDelay(attemptNumber, retryPolicyFor(attempt.retry_schedule_minutes));
  if (endpointHealthy || delayMs === null) {
    await pool.query(`UPDATE delivery_attempts SET attempt_count = $2, last_http_status = $3 WHERE id = $1`,
      [attempt.id, attemptNumber, result.httpStatus]);
    await deadLetter(attempt, endpointHealthy ? 'payload_rejected' : 'delivery_exhausted',
      endpointHealthy ? `${result.error} (endpoint answered a ping fine, so this event is the problem)` : result.error);
    return;
  }
  await pool.query(
    `UPDATE delivery_attempts
     SET attempt_count = $2, next_retry_at = NOW() + ($3 * INTERVAL '1 millisecond'),
         last_error = $4, last_http_status = $5, locked_until = NULL
     WHERE id = $1`,
    [attempt.id, attemptNumber, delayMs, result.error, result.httpStatus],
  );
  console.log(`Still failing (attempt ${attemptNumber}, ${timeoutMs}ms timeout), next try in ${Math.round(delayMs / 1000)}s:`, attempt.event_id, '->', attempt.url);
}

// An unexpected throw (not an HTTP failure - those are handled above) means something
// about this row/system is broken. Retry a few times, then park it as a processing
// error so it can't crash-loop the worker forever.
async function processSafely(attempt) {
  try {
    await processOneDelivery(attempt);
  } catch (err) {
    console.error('Processing error:', attempt.event_id, '-', err.message);
    const errors = attempt.processing_errors + 1;
    await pool.query(`UPDATE delivery_attempts SET processing_errors = $2, locked_until = NULL WHERE id = $1`, [attempt.id, errors])
      .catch(() => {}); // if even this fails, the lease expires and the row is retried
    if (errors >= MAX_PROCESSING_ERRORS) await deadLetter(attempt, 'processing_error', err.message).catch(() => {});
  }
}

let stopping = false;
let currentSweep = Promise.resolve();

async function sweep() {
  const rows = await claimDueDeliveries();
  if (rows.length === 0) return;
  console.log(`Claimed ${rows.length} due deliveries (one per endpoint, oldest first)...`);
  // Different rows here are for DIFFERENT endpoints (the query guarantees that),
  // so they have no ordering relationship to each other - safe to run at once.
  await Promise.all(rows.map(processSafely));
}

async function run() {
  await dlqProducer.connect();
  console.log(`Delivery worker started, polling every ${POLL_MS / 1000}s${FAST_MODE ? ' (FAST MODE: retry schedule in seconds)' : ''}...`);

  // Leases make overlapping sweeps safe, but skipping keeps one process from piling up.
  let isRunning = false;
  async function tick() {
    if (isRunning || stopping) return;
    isRunning = true;
    currentSweep = sweep().catch((err) => console.error('Sweep failed:', err.message));
    await currentSweep;
    isRunning = false;
  }

  await tick();
  const timer = setInterval(tick, POLL_MS);

  // Graceful shutdown: finish in-flight deliveries (and their ledger writes) first.
  for (const sig of ['SIGINT', 'SIGTERM']) {
    process.on(sig, async () => {
      if (stopping) return;
      stopping = true;
      clearInterval(timer);
      console.log('Shutting down, finishing in-flight deliveries...');
      await currentSweep;
      await dlqProducer.disconnect();
      await closeRedis();
      await pool.end();
      process.exit(0);
    });
  }
}

run().catch((err) => {
  console.error('Delivery worker failed to start:', err);
  process.exit(1);
});
