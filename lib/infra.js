// Shared clients + config. Every process imports from here instead of re-declaring them.
import 'dotenv/config';
import pg from 'pg';
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { createClient } from 'redis';
import { decrypt, encrypt } from '../core/signing.js';
import { log } from './log.js';
import { inSpan, currentTraceparent, SpanKind } from './tracing.js';

export const TOPICS = { events: 'webhooks.events', dlq: 'webhooks.dlq' };
// Upper bound on consumer parallelism (one consumer per partition at most). Raised from
// 3 after the load test showed tenants skewing onto one partition. Changing it on a
// live topic remaps tenants to partitions - see growPartitions() in migrate.js.
export const EVENT_PARTITIONS = 12;

// RETRY_FAST_MODE=true swaps minutes for seconds in every retry schedule. Testing only.
export const FAST_MODE = process.env.RETRY_FAST_MODE === 'true';
const scheduleUnitMs = FAST_MODE ? 1000 : 60_000;

// Retry policy, configurable globally (spec: a tunable default, not a hardcoded rule).
// e.g. RETRY_SCHEDULE_MINUTES=1,5,30 -> 3 retries. Defaults to 1,5,30,120,720.
// ponytail: global, not per-tenant yet - add a tenants.retry_schedule column when needed.
const schedule = process.env.RETRY_SCHEDULE_MINUTES?.split(',').map(Number);
if (schedule && (schedule.length === 0 || schedule.some((n) => !(n > 0)))) throw new Error('RETRY_SCHEDULE_MINUTES must be positive numbers');
export const retryPolicy = { unitMs: scheduleUnitMs, ...(schedule && { schedule }) };
// A tenant's own schedule (tenants.retry_schedule_minutes) overrides the global one.
export const retryPolicyFor = (tenantSchedule) => (tenantSchedule?.length ? { ...retryPolicy, schedule: tenantSchedule } : retryPolicy);

export const pool = new pg.Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  // Fail fast instead of hanging forever when the database host is unreachable (failover).
  connectionTimeoutMillis: 5000,
});
// An idle pooled connection that dies (Postgres restart/failover) is emitted here. With no
// listener, Node treats it as an unhandled 'error' event and exits: one Postgres restart
// killed every process at once (production/incident-reports/001). The pool already drops
// the dead client; in-flight queries reject on their own and each loop handles that.
pool.on('error', (err) => log.warn('postgres idle connection lost', { err: err.message }));

// Postgres is unreachable or restarting (vs. a bug or a bad query): the caller should
// answer 503 + Retry-After, because the same request will work once it's back.
const UNAVAILABLE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN',
  '57P01', '57P02', '57P03', '53300']); // admin/crash shutdown, cannot connect now, too many connections
export const isUnavailable = (err) => UNAVAILABLE_CODES.has(err?.code)
  || /Connection terminated|timeout exceeded when trying to connect/.test(err?.message);

// kafkajs's own logger prints Error objects as {} (JSON.stringify drops message/stack), so
// a failed eachBatch logged no cause at all. Route its logs through ours instead.
const kafkaLevels = { [logLevel.ERROR]: 'error', [logLevel.WARN]: 'warn', [logLevel.INFO]: 'info', [logLevel.DEBUG]: 'info' };
const kafkaLogCreator = () => ({ namespace, level, log: { message, error, stack, timestamp, logger, ...fields } }) =>
  log[kafkaLevels[level] ?? 'info'](`kafka: ${message}`, { ...fields, namespace, ...(error && { err: error }) });

export const kafka = new Kafka({
  clientId: `reliant-${process.argv[1]?.split(/[\\/]/).pop()?.replace('.js', '')}`,
  brokers: [process.env.KAFKA_BROKER],
  logLevel: logLevel.WARN,
  logCreator: kafkaLogCreator,
});

// Idempotent producer: broker drops duplicates caused by the client's own retries.
export const newProducer = () => kafka.producer({ idempotent: true, maxInFlightRequests: 1, createPartitioner: Partitioners.DefaultPartitioner });

// The topic used to be auto-created with 1 partition. Create it explicitly so a
// fresh Redpanda gets the right count. No-op if the topics already exist.
export async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const topics = [
      { topic: TOPICS.events, numPartitions: EVENT_PARTITIONS },
      { topic: TOPICS.dlq, numPartitions: 1 },
    ].filter((t) => !existing.has(t.topic));
    if (topics.length) await admin.createTopics({ topics });
  } finally {
    await admin.disconnect();
  }
}

// Group commit for Kafka publishes. kafkajs sends an idempotent producer's requests
// one at a time (~1,300 sends/s here no matter the concurrency), so one message per
// send capped ingest at ~725 events/s. Instead: while a send is in flight, queue new
// messages; when it finishes, send everything queued as ONE batch. No artificial delay
// (an idle producer sends immediately), arrival order is kept, and idempotence -
// which is what keeps retried sends from reordering a partition - stays on.
//
// Bounded wait: with the broker down, kafkajs retries one send for ~30s and everything
// queued behind it waits too, so ingest requests hung for 30s+ (incident 003). After
// PUBLISH_TIMEOUT_MS the caller gets an error (ingest answers 503: the event is already
// in Postgres), and a message not yet sent leaves the queue; retry-publisher sends it.
// One already in flight may still land later; that just sets published_to_kafka.
const PUBLISH_TIMEOUT_MS = 3000;
const batches = new WeakMap();
function sendBatched(producer, message, eventId) {
  let state = batches.get(producer);
  if (!state) batches.set(producer, (state = { queue: [], sending: false }));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      const i = state.queue.indexOf(entry);
      if (i !== -1) state.queue.splice(i, 1);
      reject(new Error(`kafka publish timed out after ${PUBLISH_TIMEOUT_MS}ms`));
    }, PUBLISH_TIMEOUT_MS);
    const done = (fn) => (v) => { clearTimeout(timer); fn(v); };
    const entry = { message, eventId, resolve: done(resolve), reject: done(reject) };
    state.queue.push(entry);
    if (!state.sending) flush(producer, state);
  });
}
async function flush(producer, state) {
  state.sending = true;
  while (state.queue.length) {
    const batch = state.queue.splice(0, 1000);
    try {
      await producer.send({ topic: TOPICS.events, messages: batch.map((b) => b.message) });
      // Mark the whole batch published in one statement, not one UPDATE per event.
      await pool.query(
        `UPDATE events SET published_to_kafka = TRUE, status = 'delivered' WHERE event_id = ANY($1::uuid[])`,
        [batch.map((b) => b.eventId)],
      );
      batch.forEach((b) => b.resolve());
    } catch (err) {
      batch.forEach((b) => b.reject(err)); // each caller answers 503; retry-publisher covers it
    }
  }
  state.sending = false;
}

// Second durability boundary: Kafka ack (acks=all is kafkajs' default), then mark it.
// Keyed by tenant_id so one tenant's events stay ordered within one partition.
export function publishEvent(producer, event) {
  return inSpan('kafka.publish', {
    kind: SpanKind.PRODUCER,
    attributes: { 'messaging.system': 'kafka', 'messaging.destination.name': TOPICS.events, 'reliant.event_id': event.event_id },
  }, () => {
    const traceparent = currentTraceparent(); // the consumer continues the trace from this header
    return sendBatched(producer, {
      key: event.tenant_id,
      value: JSON.stringify({
        event_id: event.event_id, tenant_id: event.tenant_id, type: event.type,
        payload: event.payload, created_at: event.created_at,
      }),
      ...(traceparent && { headers: { traceparent } }),
    }, event.event_id);
  });
}

let redisClient, redisConnecting;
export async function redis() {
  if (!redisClient) {
    redisClient = createClient({
      url: process.env.REDIS_URL || 'redis://localhost:6379',
      // Reject commands while disconnected instead of queueing them until Redis is back.
      // With the queue, every caller's fail-open path (rate limit, breakers) just hung:
      // a Redis outage stalled ingest and the consumer for its whole length (incident 002).
      disableOfflineQueue: true,
    });
    redisClient.on('error', (err) => log.error('redis error', { err }));
    // Settles on the first connect OR the first failure. connect() itself never settles
    // while Redis is unreachable (it keeps retrying), so awaiting it hung every caller of
    // a process that started during a Redis outage.
    redisConnecting = new Promise((resolve) => {
      redisClient.once('ready', resolve);
      redisClient.once('error', resolve);
    });
    redisClient.connect().catch(() => {});
  }
  // Every caller waits for that first attempt: without the offline queue, a command sent
  // while it's in progress would be rejected. If Redis is down, commands reject (callers
  // fail open) and the client keeps reconnecting in the background.
  await redisConnecting;
  return redisClient;
}
export const closeRedis = () => redisClient?.quit();

function encryptionKey() {
  const key = Buffer.from(process.env.SECRET_ENCRYPTION_KEY || '', 'base64');
  if (key.length !== 32) throw new Error('SECRET_ENCRYPTION_KEY must be 32 bytes, base64. Generate: node -e "console.log(require(\'crypto\').randomBytes(32).toString(\'base64\'))"');
  return key;
}
export const encryptSecret = (s) => encrypt(s, encryptionKey());

// Every delivery to an endpoint needs its secret, and AES-GCM-decrypting it per event
// was measurable CPU in the consumer. Cache by ciphertext: a rotation produces new
// ciphertext, so it can never serve a stale secret.
// ponytail: cleared wholesale at 10k entries; an LRU only if that ever churns.
const secretCache = new Map();
export function decryptSecret(blob) {
  let secret = secretCache.get(blob);
  if (secret === undefined) {
    if (secretCache.size >= 10_000) secretCache.clear();
    secret = decrypt(blob, encryptionKey());
    secretCache.set(blob, secret);
  }
  return secret;
}
