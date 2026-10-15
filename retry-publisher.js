// Safety net for the ingest path: an event saved to Postgres whose Kafka publish
// failed (Kafka down, ingest crashed mid-request) gets re-published from here.
import { pool, newProducer, publishEvent, ensureTopics, retryPolicy, FAST_MODE } from './lib/infra.js';
import { nextRetryDelay } from './core/backoff.js';
import { log } from './lib/log.js';
import { serveMetrics } from './lib/metrics.js';
import { initTracing, inSpan, fromTraceparent } from './lib/tracing.js';

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

async function run() {
  await ensureTopics();
  await producer.connect();
  initTracing('retry-publisher');
  serveMetrics(9104);
  log.info('retry publisher started', { poll_ms: 10000 });

  let isRunning = false;
  async function tick() {
    if (isRunning) return;
    isRunning = true;
    try {
      await retryStuckEvents();
    } catch (err) {
      log.error('retry sweep failed', { err });
    } finally {
      isRunning = false;
    }
  }

  await tick();
  setInterval(tick, 10000);
}

run();
