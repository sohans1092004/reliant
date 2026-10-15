// Ingest API - the only way events enter Reliant.
//   POST /events   Authorization: Bearer $INGEST_TOKEN   (internal producer services)
//   body: { event_id?, tenant_id, type, payload }
// Returns 202 only after BOTH durability boundaries: Postgres insert, then Kafka ack.
// Safe to retry on any error: the same event_id is deduplicated.
import http from 'node:http';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { pool, redis, newProducer, publishEvent, ensureTopics } from './lib/infra.js';
import { sendJson, readJson, bearer, safeEqual } from './lib/http.js';
import { validateEvent } from './core/event.js';

const PORT = Number(process.env.INGEST_PORT || 3000);
const RATE_LIMIT_PER_SEC = Number(process.env.RATE_LIMIT_PER_SEC || 1000);
const producer = newProducer();

// Per-tenant rate limit, so one tenant's spike can't starve the rest (spec §9.7).
// ponytail: fixed 1-second window, allows up to 2x burst at a window edge. Swap for a
// Lua token bucket if that burst matters. Fails open if Redis is down.
async function overRateLimit(tenantId) {
  try {
    const r = await redis();
    const key = `rl:${tenantId}:${Math.floor(Date.now() / 1000)}`;
    const count = await r.incr(key);
    if (count === 1) await r.expire(key, 2);
    return count > RATE_LIMIT_PER_SEC;
  } catch {
    return false;
  }
}

async function ingest(req, res) {
  if (!process.env.INGEST_TOKEN || !safeEqual(bearer(req), process.env.INGEST_TOKEN)) {
    return sendJson(res, 401, { error: 'invalid ingest token' });
  }
  const body = await readJson(req);
  const problems = validateEvent(body);
  if (problems.length) return sendJson(res, 400, { error: 'invalid event', problems });

  const { rows: tenant } = await pool.query('SELECT 1 FROM tenants WHERE tenant_id = $1', [body.tenant_id]);
  if (!tenant.length) return sendJson(res, 400, { error: `unknown tenant ${body.tenant_id}` });
  if (await overRateLimit(body.tenant_id)) return sendJson(res, 429, { error: 'rate limit exceeded' });

  const eventId = body.event_id || crypto.randomUUID();
  // First durability boundary. The PK makes a re-submitted event_id a no-op.
  const { rows: inserted } = await pool.query(
    `INSERT INTO events (event_id, tenant_id, type, payload, created_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (event_id) DO NOTHING
     RETURNING event_id, tenant_id, type, payload, created_at, published_to_kafka`,
    [eventId, body.tenant_id, body.type, body.payload],
  );

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
    console.error('Kafka publish failed:', eventId, err.message);
    return sendJson(res, 503, { error: 'accepted into storage but not yet queued; retry is safe', event_id: eventId });
  }
  return sendJson(res, inserted[0] ? 202 : 200, { event_id: eventId, duplicate: !inserted[0] });
}

export const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'POST' && req.url === '/events') return await ingest(req, res);
    if (req.method === 'GET' && req.url === '/health') return sendJson(res, 200, { ok: true });
    sendJson(res, 404, { error: 'not found' });
  } catch (err) {
    if (!err.status) console.error('Ingest error:', err);
    sendJson(res, err.status || 500, { error: err.status ? err.message : 'internal error' });
  }
});

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  await ensureTopics();
  await producer.connect();
  server.listen(PORT, () => console.log(`Ingest API listening on http://localhost:${PORT}`));
}
