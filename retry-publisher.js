// Safety net for the ingest path: an event saved to Postgres whose Kafka publish
// failed (Kafka down, ingest crashed mid-request) gets re-published from here.
import { pool, kafka, TOPICS, newProducer, publishEvent, ensureTopics, retryPolicy, FAST_MODE } from './lib/infra.js';
import { nextRetryDelay } from './core/backoff.js';
import { log } from './lib/log.js';
import { serveMetrics, metrics } from './lib/metrics.js';
import { initTracing, inSpan, fromTraceparent } from './lib/tracing.js';
import { onShutdown } from './lib/shutdown.js';

const producer = newProducer();

if (FAST_MODE) {
  log.warn('FAST MODE: retry schedule is in seconds, not minutes. For testing only.');
}

async function retryStuckEvents() {
  // The 30s grace period keeps us from racing an ingest request that is
  // publishing this very event right now.
  const { rows } = await pool.query(
    `SELECT event_id, tenant_id, type, payload, created_at, attempt_count, traceparent
     FROM events
     WHERE status = 'pending' AND next_retry_at <= NOW() AND created_at < NOW() - INTERVAL '30 seconds'`
  );

  if (rows.length === 0) return;
  log.info('events due for kafka publish retry', { count: rows.length });

  for (const event of rows) {
    try {
      // Joins the original ingest trace, so a late publish shows up next to the 503.
      await inSpan('publish.retry', { parent: fromTraceparent(event.traceparent), attributes: { 'reliant.event_id': event.event_id } },
        () => publishEvent(producer, event));
      log.info('published', { event_id: event.event_id, tenant_id: event.tenant_id });
    } catch (err) {
      const newAttemptCount = event.attempt_count + 1;
      const waitMs = nextRetryDelay(newAttemptCount, retryPolicy);

      if (waitMs === null) {
        await pool.query(
          `UPDATE events SET status = 'dead', attempt_count = $2 WHERE event_id = $1`,
          [event.event_id, newAttemptCount]
        );
        log.error('gave up publishing to kafka, marked dead', { event_id: event.event_id, tenant_id: event.tenant_id });
      } else {
        await pool.query(
          `UPDATE events
           SET attempt_count = $2, next_retry_at = NOW() + ($3 * INTERVAL '1 millisecond')
           WHERE event_id = $1`,
          [event.event_id, newAttemptCount, waitMs]
        );
        log.warn('kafka publish still failing', { event_id: event.event_id, attempt: newAttemptCount, next_retry_in_s: Math.round(waitMs / 1000), err });
      }
    }
  }
}

// Reconciler: finds accepted events that have NO outcome at all - no ledger row and no
// queued delivery - for an endpoint that was subscribed when they arrived. ADR-005 says
// that can't happen once the consumer has committed past them, so one means something
// broke the durability chain (e.g. the broker lost an acked message). Those events are
// sent back to Kafka through the normal publish path above.
//  - Only while the consumer group is nearly caught up (lag < RECONCILE_MAX_LAG): during
//    a backlog, an old event with no outcome is usually just still waiting in Kafka.
//    Re-sending it then would only duplicate it. reliant_unaccounted_events stays up and
//    ReliantUnaccountedEvents fires if this lasts.
//  - A false positive costs one duplicate delivery, which at-least-once already allows.
//    (An endpoint whose event_types changed after the event arrived can cause one.)
// ponytail: rescans a 45-minute window every 5 min; at sustained 1,000/s that's ~2.7M rows
// a pass. Past that, store each event's Kafka offset at publish and check only events
// below the group's committed offset.
const RECONCILE_EVERY_MS = Number(process.env.RECONCILE_EVERY_MS || 5 * 60_000);
const RECONCILE_AFTER_MIN = Number(process.env.RECONCILE_AFTER_MIN || 15);
const RECONCILE_MAX_LAG = Number(process.env.RECONCILE_MAX_LAG || 1000);
const admin = kafka.admin();

export async function findUnaccounted(afterMin = RECONCILE_AFTER_MIN) {
  const { rows } = await pool.query(
    `SELECT DISTINCT e.event_id
     FROM events e
     JOIN endpoints ep ON ep.tenant_id = e.tenant_id AND ep.is_active AND ep.created_at <= e.created_at
                      AND (ep.event_types IS NULL OR e.type = ANY (ep.event_types))
     WHERE e.created_at > NOW() - (($1 + 45) * INTERVAL '1 minute') AND e.created_at < NOW() - ($1 * INTERVAL '1 minute')
       AND e.status <> 'pending'  -- still waiting for its first publish: the sweep above owns it
       AND NOT EXISTS (SELECT 1 FROM delivery_attempts da WHERE da.event_id = e.event_id AND da.endpoint_id = ep.endpoint_id)
       AND NOT EXISTS (SELECT 1 FROM delivery_ledger l WHERE l.event_id = e.event_id AND l.endpoint_id = ep.endpoint_id)
     LIMIT 10000`,
    [afterMin]);
  return rows.map((r) => r.event_id);
}

async function consumerLag() {
  const [ends, [{ partitions }]] = await Promise.all([
    admin.fetchTopicOffsets(TOPICS.events),
    admin.fetchOffsets({ groupId: 'delivery-workers', topics: [TOPICS.events] }),
  ]);
  const committed = new Map(partitions.map((p) => [p.partition, Number(p.offset)]));
  return ends.reduce((sum, p) => sum + Math.max(0, Number(p.high) - Math.max(0, committed.get(p.partition) ?? 0)), 0);
}

async function reconcile() {
  const ids = await findUnaccounted();
  metrics.unaccountedEvents.set(ids.length);
  if (!ids.length) return;
  const lag = await consumerLag();
  if (lag >= RECONCILE_MAX_LAG) {
    log.warn('events with no outcome, but the consumer is behind: not re-sending yet', { count: ids.length, lag });
    return;
  }
  // Back to 'pending' = the sweep above publishes them on its next pass.
  await pool.query(`UPDATE events SET status = 'pending', attempt_count = 0, next_retry_at = NOW() WHERE event_id = ANY ($1)`, [ids]);
  metrics.reconciled.inc(ids.length);
  log.error('accepted events had no outcome; re-sending them to Kafka', { count: ids.length, lag, sample: ids.slice(0, 5) });
}

async function run() {
  // Registered first, so a SIGTERM during startup or the first sweep isn't ignored.
  let isRunning = false, stopping = false, current = Promise.resolve(), timer, reconcileTimer;
  onShutdown(async () => {
    stopping = true;
    clearInterval(timer);
    clearInterval(reconcileTimer);
    await current;
    await producer.disconnect();
    await admin.disconnect();
    await pool.end();
  });
  await ensureTopics();
  await producer.connect();
  await admin.connect();
  initTracing('retry-publisher');
  serveMetrics(9104);
  log.info('retry publisher started', { poll_ms: 10000 });

  async function tick() {
    if (isRunning || stopping) return;
    isRunning = true;
    current = retryStuckEvents().catch((err) => log.error('retry sweep failed', { err }));
    await current;
    isRunning = false;
  }

  timer = setInterval(tick, 10000);
  await tick();

  let reconciling = false;
  reconcileTimer = setInterval(async () => {
    if (reconciling || stopping) return;
    reconciling = true;
    await reconcile().catch((err) => log.error('reconcile failed', { err }));
    reconciling = false;
  }, RECONCILE_EVERY_MS);
}

if (process.argv[1]?.endsWith('retry-publisher.js')) run();
