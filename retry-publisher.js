// Safety net for the ingest path: an event saved to Postgres whose Kafka publish
// failed (Kafka down, ingest crashed mid-request) gets re-published from here.
import { pool, newProducer, publishEvent, ensureTopics, retryPolicy, FAST_MODE } from './lib/infra.js';
import { nextRetryDelay } from './core/backoff.js';

const producer = newProducer();

if (FAST_MODE) {
  console.log('FAST MODE: schedule is in seconds, not minutes. For testing only.');
}

async function retryStuckEvents() {
  // The 30s grace period keeps us from racing an ingest request that is
  // publishing this very event right now.
  const { rows } = await pool.query(
    `SELECT event_id, tenant_id, type, payload, created_at, attempt_count
     FROM events
     WHERE status = 'pending' AND next_retry_at <= NOW() AND created_at < NOW() - INTERVAL '30 seconds'`
  );

  if (rows.length === 0) return;
  console.log(`Found ${rows.length} event(s) due for retry...`);

  for (const event of rows) {
    try {
      await publishEvent(producer, event);
      console.log('Published:', event.event_id);
    } catch (err) {
      const newAttemptCount = event.attempt_count + 1;
      const waitMs = nextRetryDelay(newAttemptCount, retryPolicy);

      if (waitMs === null) {
        await pool.query(
          `UPDATE events SET status = 'dead', attempt_count = $2 WHERE event_id = $1`,
          [event.event_id, newAttemptCount]
        );
        console.log('Gave up, marked dead:', event.event_id);
      } else {
        await pool.query(
          `UPDATE events
           SET attempt_count = $2, next_retry_at = NOW() + ($3 * INTERVAL '1 millisecond')
           WHERE event_id = $1`,
          [event.event_id, newAttemptCount, waitMs]
        );
        console.log(`Still failing (attempt ${newAttemptCount}), next try in ${Math.round(waitMs / 1000)}s:`, event.event_id, '-', err.message);
      }
    }
  }
}

async function run() {
  await ensureTopics();
  await producer.connect();
  console.log('Retry publisher started, checking every 10 seconds for due events...');

  let isRunning = false;
  async function tick() {
    if (isRunning) return;
    isRunning = true;
    try {
      await retryStuckEvents();
    } catch (err) {
      console.error('Retry sweep failed:', err.message);
    } finally {
      isRunning = false;
    }
  }

  await tick();
  setInterval(tick, 10000);
}

run();
