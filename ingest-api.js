// Ingest API - the only way events enter Reliant.
//   POST /events   Authorization: Bearer $INGEST_TOKEN   (internal producer services)
//   body: { event_id?, tenant_id, type, payload }
// Returns 202 only after BOTH durability boundaries: Postgres insert, then Kafka ack.
// Safe to retry on any error: the same event_id is deduplicated.
import http from 'node:http';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { pool, newProducer, publishEvent, ensureTopics, closeRedis } from './lib/infra.js';
import { sendJson, readJson, bearer, safeEqual } from './lib/http.js';
import { takeToken } from './lib/rate-limit.js';
import { log } from './lib/log.js';
import { metrics, serveMetrics } from './lib/metrics.js';
import { initTracing, inSpan, currentTraceparent, SpanKind } from './lib/tracing.js';
import { onShutdown } from './lib/shutdown.js';
import { validateEvent } from './core/event.js';

const PORT = Number(process.env.INGEST_PORT || 3000);
const RATE_LIMIT_PER_SEC = Number(process.env.RATE_LIMIT_PER_SEC || 1000);
const RATE_LIMIT_BURST = Number(process.env.RATE_LIMIT_BURST || RATE_LIMIT_PER_SEC * 2);
const producer = newProducer();

// Per-tenant limits (tenants.rate_limit_*, NULL = env default). Cached for 30s so the
// hot path doesn't pay a Postgres round trip per request; a changed limit applies
// within 30s.
const limitCache = new Map();
async function tenantLimits(tenantId) {
  const hit = limitCache.get(tenantId);
  if (hit && hit.expires > Date.now()) return hit;
  const { rows: [t] } = await pool.query('SELECT rate_limit_per_sec, rate_limit_burst FROM tenants WHERE tenant_id = $1', [tenantId]);
  const rate = t?.rate_limit_per_sec ?? RATE_LIMIT_PER_SEC;
  const limits = { rate, burst: t?.rate_limit_burst ?? Math.max(RATE_LIMIT_BURST, rate), expires: Date.now() + 30_000 };
  if (limitCache.size > 10_000) limitCache.clear();
  limitCache.set(tenantId, limits);
  return limits;
}

// Per-tenant token bucket, so one tenant's spike can't starve the rest (spec §9.7).
async function overRateLimit(tenantId) {
  const { rate, burst } = await tenantLimits(tenantId);
  return !(await takeToken(tenantId, rate, burst));
}

async function ingest(req, res) {
  if (!process.env.INGEST_TOKEN || !safeEqual(bearer(req), process.env.INGEST_TOKEN)) {
    return sendJson(res, 401, { error: 'invalid ingest token' });
  }
  const body = await readJson(req);
  const problems = validateEvent(body);
  if (problems.length) return sendJson(res, 400, { error: 'invalid event', problems });

  if (await overRateLimit(body.tenant_id)) {
    res.setHeader('Retry-After', '1');
    return sendJson(res, 429, { error: 'rate limit exceeded' });
  }

  const eventId = body.event_id || crypto.randomUUID();
  // First durability boundary. The PK makes a re-submitted event_id a no-op, and the
  // tenants FK rejects unknown tenants in the same round trip (no separate lookup).
  const { rows: inserted } = await pool.query(
    `INSERT INTO events (event_id, tenant_id, type, payload, created_at, traceparent)
     VALUES ($1, $2, $3, $4, NOW(), $5)
     ON CONFLICT (event_id) DO NOTHING
     RETURNING event_id, tenant_id, type, payload, created_at, published_to_kafka`,
    [eventId, body.tenant_id, body.type, body.payload, currentTraceparent()],
  ).catch((err) => {
    if (err.code === '23503') throw Object.assign(new Error(`unknown tenant ${body.tenant_id}`), { status: 400 });
    throw err;
  });

  let event = inserted[0];
  if (!event) {
    const { rows } = await pool.query(
      'SELECT event_id, tenant_id, type, payload, created_at, published_to_kafka FROM events WHERE event_id = $1',
      [eventId],
    );
    event = rows[0];
    if (event.tenant_id !== body.tenant_id) return sendJson(res, 409, { error: 'event_id already used' });
    // Duplicate of an event that already made it all the way through.
    if (event.published_to_kafka) return sendJson(res, 200, { event_id: eventId, duplicate: true });
    // Otherwise a previous attempt saved it but never reached Kafka - finish the job.
  }

  try {
    await publishEvent(producer, event); // second durability boundary
  } catch (err) {
    // Saved but not published: retry-publisher will pick it up, and a client retry is safe.
    log.error('kafka publish failed, retry-publisher will finish it', { event_id: eventId, tenant_id: body.tenant_id, err });
    return sendJson(res, 503, { error: 'accepted into storage but not yet queued; retry is safe', event_id: eventId });
  }
  return sendJson(res, inserted[0] ? 202 : 200, { event_id: eventId, duplicate: !inserted[0] });
}

export const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'POST' && req.url === '/events') {
      const done = metrics.ingestDuration.startTimer();
      res.on('finish', () => { done(); metrics.ingestRequests.inc({ status: String(res.statusCode) }); });
      return await inSpan('ingest', { kind: SpanKind.SERVER, attributes: { 'http.route': '/events' } }, async (span) => {
        await ingest(req, res);
        span.setAttribute('http.response.status_code', res.statusCode); // set by sendJson; span still open here
      });
    }
    if (req.method === 'GET' && req.url === '/health') return sendJson(res, 200, { ok: true });
    sendJson(res, 404, { error: 'not found' });
  } catch (err) {
    if (!err.status) log.error('ingest error', { err, stack: err.stack });
    sendJson(res, err.status || 500, { error: err.status ? err.message : 'internal error' });
  }
});

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  // Registered first, so a SIGTERM during startup isn't ignored. Stop accepting; requests
  // already in flight finish both durability boundaries. (close() on a server that never
  // started listening just calls back with an error - fine here.)
  onShutdown(async () => {
    await new Promise((resolve) => server.close(() => resolve()));
    await producer.disconnect();
    await closeRedis();
    await pool.end();
  });
  await ensureTopics();
  await producer.connect();
  initTracing('ingest');
  serveMetrics(9100);
  server.listen(PORT, () => log.info('ingest API listening', { port: PORT }));
}
