// Shared clients + config. Every process imports from here instead of re-declaring them.
import 'dotenv/config';
import pg from 'pg';
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { createClient } from 'redis';
import { decrypt, encrypt } from '../core/signing.js';

export const TOPICS = { events: 'webhooks.events', dlq: 'webhooks.dlq' };
export const EVENT_PARTITIONS = 3;

// RETRY_FAST_MODE=true swaps minutes for seconds in every retry schedule. Testing only.
export const FAST_MODE = process.env.RETRY_FAST_MODE === 'true';
const scheduleUnitMs = FAST_MODE ? 1000 : 60_000;

// Retry policy, configurable globally (spec: a tunable default, not a hardcoded rule).
// e.g. RETRY_SCHEDULE_MINUTES=1,5,30 -> 3 retries. Defaults to 1,5,30,120,720.
// ponytail: global, not per-tenant yet - add a tenants.retry_schedule column when needed.
const schedule = process.env.RETRY_SCHEDULE_MINUTES?.split(',').map(Number);
if (schedule && (schedule.length === 0 || schedule.some((n) => !(n > 0)))) throw new Error('RETRY_SCHEDULE_MINUTES must be positive numbers');
export const retryPolicy = { unitMs: scheduleUnitMs, ...(schedule && { schedule }) };

export const pool = new pg.Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

export const kafka = new Kafka({
  clientId: `reliant-${process.argv[1]?.split(/[\\/]/).pop()?.replace('.js', '')}`,
  brokers: [process.env.KAFKA_BROKER],
  logLevel: logLevel.WARN,
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
const batches = new WeakMap();
function sendBatched(producer, message, eventId) {
  let state = batches.get(producer);
  if (!state) batches.set(producer, (state = { queue: [], sending: false }));
  return new Promise((resolve, reject) => {
    state.queue.push({ message, eventId, resolve, reject });
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
  return sendBatched(producer, {
    key: event.tenant_id,
    value: JSON.stringify({
      event_id: event.event_id, tenant_id: event.tenant_id, type: event.type,
      payload: event.payload, created_at: event.created_at,
    }),
  }, event.event_id);
}

let redisClient;
export async function redis() {
  if (!redisClient) {
    redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
    redisClient.on('error', (err) => console.error('Redis error:', err.message));
    await redisClient.connect();
  }
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
