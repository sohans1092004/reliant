// Tenant-facing Dashboard API + UI.  Authorization: Bearer <tenant API key>
//
//   GET   /                                          dashboard UI
//   GET   /api/endpoints                             list endpoints (+ circuit breaker state)
//   POST  /api/endpoints              {url}          create; signing secret returned ONCE
//   PATCH /api/endpoints/:id          {is_active}    enable / disable
//   POST  /api/endpoints/:id/rotate-secret           new secret returned ONCE; old one keeps
//                                                    verifying for 24h (dual-secret window)
//   GET   /api/endpoints/:id/deliveries              recent attempt history (ledger)
//   GET   /api/endpoints/:id/deliveries/:event_id    "what happened to my webhook"
//   POST  /api/deliveries/:event_id/:endpoint_id/replay   one (event, endpoint) pair, never a broadcast
//   POST  /api/endpoints/:id/replay-dead  {reason?}  re-queue this endpoint's dead letters, original order
//   GET   /api/dlq                                   dead-lettered deliveries
//   GET   /api/settings                              this tenant's retry policy
//   PUT   /api/settings   {retry_schedule_minutes}   own schedule, e.g. [1, 5, 30]; null = default
//
// Tenant isolation: tenant_id comes ONLY from the API key, and every query filters on it.
// Anything belonging to another tenant is a 404, same as if it didn't exist.
import http from 'node:http';
import fs from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { pool, encryptSecret, retryPolicy } from './lib/infra.js';
import { getBreaker } from './lib/breaker.js';
import { assertSafeUrl } from './lib/deliver.js';
import { sendJson, readJson, bearer, sha256 } from './lib/http.js';
import { log } from './lib/log.js';
import { serveMetrics } from './lib/metrics.js';
import { generateSecret } from './core/signing.js';
import { validateRetrySchedule, DEFAULT_SCHEDULE_MINUTES } from './core/backoff.js';

const PORT = Number(process.env.DASHBOARD_PORT || 3001);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const notFound = () => Object.assign(new Error('not found'), { status: 404 });
const badRequest = (m) => Object.assign(new Error(m), { status: 400 });

async function authenticate(req) {
  const key = bearer(req);
  if (!key) return null;
  const { rows } = await pool.query('SELECT tenant_id FROM tenants WHERE api_key_hash = $1', [sha256(key)]);
  return rows[0]?.tenant_id ?? null;
}

async function ownEndpoint(tenantId, endpointId) {
  if (!UUID.test(endpointId)) throw notFound();
  const { rows } = await pool.query(
    'SELECT endpoint_id, url, is_active, created_at, secret_rotated_at FROM endpoints WHERE endpoint_id = $1 AND tenant_id = $2',
    [endpointId, tenantId],
  );
  if (!rows[0]) throw notFound();
  return rows[0];
}

async function validateUrl(url) {
  try {
    if (!/^https?:\/\//.test(url)) throw new Error('must be http(s)');
    await assertSafeUrl(url);
  } catch (err) {
    throw badRequest(`invalid url: ${err.message}`);
  }
}

// delivery_attempts row (if any) wins; otherwise a clean inline delivery lives only in the ledger.
async function deliveryState(tenantId, eventId, endpointId) {
  const { rows: [latest] } = await pool.query(
    `SELECT status, http_status_code, attempted_at, error_detail FROM delivery_ledger
     WHERE event_id = $1 AND endpoint_id = $2 AND tenant_id = $3 ORDER BY attempted_at DESC, id DESC LIMIT 1`,
    [eventId, endpointId, tenantId],
  );
  const { rows: [da] } = await pool.query(
    `SELECT da.status, da.attempt_count, da.next_retry_at, da.terminal_at, da.dead_reason, da.last_error
     FROM delivery_attempts da JOIN endpoints ep USING (endpoint_id)
     WHERE da.event_id = $1 AND da.endpoint_id = $2 AND ep.tenant_id = $3`,
    [eventId, endpointId, tenantId],
  );
  if (!da && !latest) return null;
  const state = !da ? 'delivered'
    : da.status === 'dead' ? 'dead_lettered'
    : da.status === 'delivered' ? 'delivered'
    : da.attempt_count > 0 ? 'retrying' : 'queued';
  return {
    event_id: eventId, endpoint_id: endpointId, current_state: state,
    attempt_count: da?.attempt_count ?? 1,
    latest_attempt: latest ?? null,
    next_retry_at: state === 'retrying' || state === 'queued' ? da.next_retry_at : null,
    terminal_at: da ? da.terminal_at : latest.attempted_at,
    dead_reason: da?.dead_reason ?? null,
  };
}

const routes = [
  ['GET', /^\/api\/endpoints$/, async (t) => {
    const { rows } = await pool.query(
      'SELECT endpoint_id, url, is_active, created_at, secret_rotated_at FROM endpoints WHERE tenant_id = $1 ORDER BY created_at',
      [t],
    );
    for (const ep of rows) ep.circuit = (await getBreaker(ep.endpoint_id).catch(() => ({ state: 'unknown' }))).state;
    return rows;
  }],

  ['POST', /^\/api\/endpoints$/, async (t, _, body) => {
    await validateUrl(body.url);
    const secret = generateSecret();
    const { rows } = await pool.query(
      `INSERT INTO endpoints (tenant_id, url, secret_encrypted) VALUES ($1, $2, $3)
       RETURNING endpoint_id, url, is_active, created_at`,
      [t, body.url, encryptSecret(secret)],
    );
    return [201, { ...rows[0], secret, note: 'Store this secret now - it will not be shown again.' }];
  }],

  ['PATCH', /^\/api\/endpoints\/([^/]+)$/, async (t, [id], body) => {
    await ownEndpoint(t, id);
    if (typeof body.is_active !== 'boolean') throw badRequest('is_active must be a boolean');
    const { rows } = await pool.query(
      'UPDATE endpoints SET is_active = $3 WHERE endpoint_id = $1 AND tenant_id = $2 RETURNING endpoint_id, url, is_active',
      [id, t, body.is_active],
    );
    return rows[0];
  }],

  ['POST', /^\/api\/endpoints\/([^/]+)\/rotate-secret$/, async (t, [id]) => {
    await ownEndpoint(t, id);
    const secret = generateSecret();
    await pool.query(
      `UPDATE endpoints SET secret_previous_encrypted = secret_encrypted, secret_encrypted = $3, secret_rotated_at = NOW()
       WHERE endpoint_id = $1 AND tenant_id = $2`,
      [id, t, encryptSecret(secret)],
    );
    return { endpoint_id: id, secret, note: 'The previous secret keeps working for 24 hours. Store this one now - it will not be shown again.' };
  }],

  ['GET', /^\/api\/endpoints\/([^/]+)\/deliveries$/, async (t, [id]) => {
    await ownEndpoint(t, id);
    const { rows } = await pool.query(
      `SELECT l.event_id, e.type, l.attempt_number, l.status, l.http_status_code, l.latency_ms, l.error_detail, l.attempted_at
       FROM delivery_ledger l JOIN events e USING (event_id)
       WHERE l.tenant_id = $1 AND l.endpoint_id = $2 ORDER BY l.attempted_at DESC LIMIT 100`,
      [t, id],
    );
    return rows;
  }],

  ['GET', /^\/api\/endpoints\/([^/]+)\/deliveries\/([^/]+)$/, async (t, [id, eventId]) => {
    await ownEndpoint(t, id);
    if (!UUID.test(eventId)) throw notFound();
    return (await deliveryState(t, eventId, id)) ?? Promise.reject(notFound());
  }],

  ['POST', /^\/api\/deliveries\/([^/]+)\/([^/]+)\/replay$/, async (t, [eventId, id]) => {
    await ownEndpoint(t, id);
    if (!UUID.test(eventId)) throw notFound();
    const { rows: ev } = await pool.query('SELECT 1 FROM events WHERE event_id = $1 AND tenant_id = $2', [eventId, t]);
    if (!ev.length) throw notFound();
    // Re-queue just this pair, at the back of the endpoint's queue (created_at = NOW()),
    // with a fresh retry budget. Works on any event still in the Postgres archive,
    // regardless of Kafka retention.
    const { rows } = await pool.query(
      `INSERT INTO delivery_attempts (event_id, endpoint_id) VALUES ($1, $2)
       ON CONFLICT (event_id, endpoint_id) DO UPDATE SET
         status = 'pending', attempt_count = 0, next_retry_at = NOW(), created_at = NOW(),
         dead_reason = NULL, terminal_at = NULL, last_error = NULL, last_http_status = NULL,
         processing_errors = 0, locked_until = NULL
       WHERE delivery_attempts.status <> 'pending'
       RETURNING id`,
      [eventId, id],
    );
    if (!rows.length) return [409, { error: 'delivery is already pending' }];
    return [202, { event_id: eventId, endpoint_id: id, current_state: 'queued' }];
  }],

  // Bulk replay: every dead-lettered delivery of ONE endpoint (optionally one reason),
  // e.g. after the customer fixed the handler bug behind a run of payload_rejected.
  // Still scoped to one endpoint - never a broadcast to other endpoints.
  ['POST', /^\/api\/endpoints\/([^/]+)\/replay-dead$/, async (t, [id], body) => {
    await ownEndpoint(t, id);
    const reasons = ['delivery_exhausted', 'payload_rejected', 'processing_error'];
    if (body.reason !== undefined && !reasons.includes(body.reason)) throw badRequest(`reason must be one of ${reasons.join(', ')}`);
    // One statement, so they all get the same created_at; the worker then breaks the
    // tie by id, i.e. the order they were originally queued in. NOW() puts them behind
    // anything already pending for this endpoint.
    const { rowCount } = await pool.query(
      `UPDATE delivery_attempts da SET
         status = 'pending', attempt_count = 0, next_retry_at = NOW(), created_at = NOW(),
         dead_reason = NULL, terminal_at = NULL, last_error = NULL, last_http_status = NULL,
         processing_errors = 0, locked_until = NULL
       FROM endpoints ep
       WHERE da.endpoint_id = ep.endpoint_id AND ep.tenant_id = $1 AND da.endpoint_id = $2
         AND da.status = 'dead' AND ($3::text IS NULL OR da.dead_reason = $3)`,
      [t, id, body.reason ?? null],
    );
    return [202, { endpoint_id: id, reason: body.reason ?? 'any', requeued: rowCount }];
  }],

  ['GET', /^\/api\/settings$/, async (t) => {
    const { rows: [s] } = await pool.query('SELECT retry_schedule_minutes FROM tenants WHERE tenant_id = $1', [t]);
    return { retry_schedule_minutes: s.retry_schedule_minutes, default_retry_schedule_minutes: retryPolicy.schedule ?? DEFAULT_SCHEDULE_MINUTES };
  }],

  // Only the retry schedule is tenant-settable. Rate limits are operator-only, so a
  // tenant can't raise its own share of ingest.
  ['PUT', /^\/api\/settings$/, async (t, _, body) => {
    const schedule = body.retry_schedule_minutes;
    if (schedule !== null) {
      const problem = validateRetrySchedule(schedule);
      if (problem) throw badRequest(`retry_schedule_minutes ${problem}`);
    }
    await pool.query('UPDATE tenants SET retry_schedule_minutes = $2 WHERE tenant_id = $1', [t, schedule]);
    return { retry_schedule_minutes: schedule, note: 'Applies to the next retry of every delivery, including ones already waiting.' };
  }],

  ['GET', /^\/api\/dlq$/, async (t) => {
    const { rows } = await pool.query(
      `SELECT da.event_id, da.endpoint_id, ep.url, e.type, da.dead_reason, da.last_error, da.attempt_count, da.terminal_at
       FROM delivery_attempts da JOIN endpoints ep USING (endpoint_id) JOIN events e USING (event_id)
       WHERE ep.tenant_id = $1 AND da.status = 'dead' ORDER BY da.terminal_at DESC LIMIT 200`,
      [t],
    );
    return rows;
  }],
];

export const server = http.createServer(async (req, res) => {
  try {
    const path = new URL(req.url, 'http://x').pathname;
    if (req.method === 'GET' && (path === '/' || path === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(await fs.readFile(new URL('./public/dashboard.html', import.meta.url)));
    }
    for (const [method, pattern, handler] of routes) {
      const match = path.match(pattern);
      if (!match || req.method !== method) continue;
      const tenantId = await authenticate(req);
      if (!tenantId) return sendJson(res, 401, { error: 'invalid API key' });
      const body = method === 'GET' ? {} : await readJson(req);
      const result = await handler(tenantId, match.slice(1).map(decodeURIComponent), body);
      return Array.isArray(result) && typeof result[0] === 'number'
        ? sendJson(res, result[0], result[1])
        : sendJson(res, 200, result);
    }
    sendJson(res, 404, { error: 'not found' });
  } catch (err) {
    if (!err.status) log.error('dashboard error', { err, stack: err.stack });
    sendJson(res, err.status || 500, { error: err.status ? err.message : 'internal error' });
  }
});

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  serveMetrics(9103);
  server.listen(PORT, () => log.info('dashboard listening', { port: PORT }));
}
