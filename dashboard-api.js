// Tenant-facing Dashboard API + UI. Two ways to authenticate (lib/auth.js):
//   - people: POST /api/auth/login, then the session cookie. Owners can do everything below;
//     viewers only the GETs (plus their own password and logout).
//   - scripts: Authorization: Bearer <tenant API key>, with owner rights.
//
//   POST  /api/auth/login  {email, password}        sets the session cookie
//   POST  /api/auth/logout
//   GET   /api/auth/me                               who am I: email, role, tenant
//   POST  /api/auth/password {current_password, new_password}   signs out your other sessions
//   GET   /api/users                                 owners: this tenant's users
//   POST  /api/users {email, role}                   owners: add a user; password returned ONCE
//   PATCH /api/users/:id {role}                      owners: change a role (never the last owner)
//   DELETE /api/users/:id                            owners: remove a user (never the last owner)
//
//   GET   /                                          dashboard UI
//   GET   /api/endpoints                             list endpoints (+ circuit breaker state)
//   POST  /api/endpoints   {url, event_types?}       create; signing secret returned ONCE
//   PATCH /api/endpoints/:id {is_active?, event_types?}  enable / disable, change subscription
//                                    (event_types: list of types, or null = every type)
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
// Tenant isolation: tenant_id comes ONLY from the credential, and every query filters on it.
// Anything belonging to another tenant is a 404, same as if it didn't exist.
import http from 'node:http';
import fs from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { pool, encryptSecret, retryPolicy, closeRedis } from './lib/infra.js';
import { getBreaker } from './lib/breaker.js';
import { assertSafeUrl } from './lib/deliver.js';
import { sendJson, readJson } from './lib/http.js';
import { authenticate, login, sameOriginJson, sessionCookie, clearedCookie, EMAIL } from './lib/auth.js';
import { hashPassword, verifyPassword, passwordProblem, generatePassword } from './core/password.js';
import { log } from './lib/log.js';
import { serveMetrics } from './lib/metrics.js';
import { onShutdown } from './lib/shutdown.js';
import { generateSecret } from './core/signing.js';
import { validateRetrySchedule, DEFAULT_SCHEDULE_MINUTES } from './core/backoff.js';
import { validateEventTypes } from './core/event.js';

const PORT = Number(process.env.DASHBOARD_PORT || 3001);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const notFound = () => Object.assign(new Error('not found'), { status: 404 });
const badRequest = (m) => Object.assign(new Error(m), { status: 400 });

const forbidden = (m) => Object.assign(new Error(m), { status: 403 });
const ANY_ROLE = ['owner', 'viewer'];
const lastOwner = () => Object.assign(new Error('a tenant must keep at least one owner'), { status: 409 });

// Behind a load balancer every login would come from its address and share one per-IP
// limit. With TRUST_PROXY=true the client address is the last X-Forwarded-For entry (the one
// the balancer itself appended); never trust that header without a proxy in front.
const clientIp = (req) => (process.env.TRUST_PROXY === 'true'
  && (req.headers['x-forwarded-for'] || '').split(',').pop().trim()) || req.socket.remoteAddress;

// Changes to a tenant's users run one at a time per tenant (a lock on its tenants row), so
// two owners demoting each other at the same moment can't leave the tenant with none.
async function changeUser(tenantId, userId, change) {
  if (!UUID.test(userId)) throw notFound();
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    await db.query('SELECT 1 FROM tenants WHERE tenant_id = $1 FOR UPDATE', [tenantId]);
    const { rows: [user] } = await db.query('SELECT role FROM users WHERE user_id = $1 AND tenant_id = $2', [userId, tenantId]);
    if (!user) throw notFound();
    const { rows: [{ owners }] } = await db.query(`SELECT count(*)::int AS owners FROM users WHERE tenant_id = $1 AND role = 'owner'`, [tenantId]);
    const result = await change(db, user, owners);
    await db.query('COMMIT');
    return result;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    db.release();
  }
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

// [method, path, handler(tenantId, params, body, who), roles allowed]. Roles default to
// both for GET and owners only for anything that changes state.
const routes = [
  ['GET', /^\/api\/auth\/me$/, async (t, _, __, who) => ({ tenant_id: t, role: who.role, via: who.via, email: who.email ?? null })],

  ['POST', /^\/api\/auth\/logout$/, async (t, _, __, who) => {
    if (who.tokenHash) await pool.query('DELETE FROM sessions WHERE token_hash = $1', [who.tokenHash]);
    return [200, { signed_out: true }, { 'Set-Cookie': clearedCookie() }];
  }, ANY_ROLE],

  ['POST', /^\/api\/auth\/password$/, async (t, _, body, who) => {
    if (who.via !== 'session') throw badRequest('sign in with email and password to change a password');
    const { rows: [u] } = await pool.query('SELECT password_hash FROM users WHERE user_id = $1', [who.userId]);
    if (!(await verifyPassword(String(body.current_password ?? ''), u.password_hash))) throw forbidden('current password is wrong');
    const problem = passwordProblem(body.new_password, who.email);
    if (problem) throw badRequest(problem);
    await pool.query('UPDATE users SET password_hash = $2 WHERE user_id = $1', [who.userId, await hashPassword(body.new_password)]);
    // Anyone else signed in as this user (a stolen cookie, a forgotten laptop) is signed out.
    const { rowCount } = await pool.query('DELETE FROM sessions WHERE user_id = $1 AND token_hash <> $2', [who.userId, who.tokenHash]);
    return { changed: true, other_sessions_signed_out: rowCount };
  }, ANY_ROLE],

  ['GET', /^\/api\/users$/, async (t) => (await pool.query(
    'SELECT user_id, email, role, created_at FROM users WHERE tenant_id = $1 ORDER BY created_at', [t])).rows, ['owner']],

  ['POST', /^\/api\/users$/, async (t, _, body) => {
    const email = String(body.email ?? '').trim().toLowerCase();
    if (!EMAIL.test(email)) throw badRequest('a valid email is required');
    if (!ANY_ROLE.includes(body.role)) throw badRequest('role must be owner or viewer');
    const password = generatePassword();
    try {
      const { rows: [u] } = await pool.query(
        'INSERT INTO users (tenant_id, email, password_hash, role) VALUES ($1, $2, $3, $4) RETURNING user_id, email, role, created_at',
        [t, email, await hashPassword(password), body.role]);
      return [201, { ...u, password, note: 'Give this password to the user now - it will not be shown again. They should change it after signing in.' }];
    } catch (err) {
      if (err.code === '23505') return [409, { error: 'that email already has an account' }];
      throw err;
    }
  }],

  ['PATCH', /^\/api\/users\/([^/]+)$/, async (t, [id], body) => {
    if (!ANY_ROLE.includes(body.role)) throw badRequest('role must be owner or viewer');
    return changeUser(t, id, async (db, user, owners) => {
      if (user.role === 'owner' && body.role !== 'owner' && owners === 1) throw lastOwner();
      // Takes effect on the user's next request: sessions look the role up every time.
      const { rows: [u] } = await db.query('UPDATE users SET role = $2 WHERE user_id = $1 RETURNING user_id, email, role', [id, body.role]);
      return u;
    });
  }],

  ['DELETE', /^\/api\/users\/([^/]+)$/, async (t, [id]) => changeUser(t, id, async (db, user, owners) => {
    if (user.role === 'owner' && owners === 1) throw lastOwner();
    await db.query('DELETE FROM users WHERE user_id = $1', [id]); // their sessions go too (ON DELETE CASCADE)
    return { user_id: id, deleted: true };
  })],

  ['GET', /^\/api\/endpoints$/, async (t) => {
    const { rows } = await pool.query(
      'SELECT endpoint_id, url, is_active, disabled_reason, disabled_at, event_types, created_at, secret_rotated_at FROM endpoints WHERE tenant_id = $1 ORDER BY created_at',
      [t],
    );
    for (const ep of rows) ep.circuit = (await getBreaker(ep.endpoint_id).catch(() => ({ state: 'unknown' }))).state;
    return rows;
  }],

  ['POST', /^\/api\/endpoints$/, async (t, _, body) => {
    await validateUrl(body.url);
    const eventTypes = body.event_types ?? null;
    const problem = validateEventTypes(eventTypes);
    if (problem) throw badRequest(problem);
    const secret = generateSecret();
    const { rows } = await pool.query(
      `INSERT INTO endpoints (tenant_id, url, secret_encrypted, event_types) VALUES ($1, $2, $3, $4)
       RETURNING endpoint_id, url, is_active, event_types, created_at`,
      [t, body.url, await encryptSecret(secret), eventTypes],
    );
    return [201, { ...rows[0], secret, note: 'Store this secret now - it will not be shown again.' }];
  }],

  ['PATCH', /^\/api\/endpoints\/([^/]+)$/, async (t, [id], body) => {
    await ownEndpoint(t, id);
    const setActive = 'is_active' in body, setTypes = 'event_types' in body;
    if (!setActive && !setTypes) throw badRequest('nothing to change: send is_active and/or event_types');
    if (setActive && typeof body.is_active !== 'boolean') throw badRequest('is_active must be a boolean');
    const problem = setTypes && validateEventTypes(body.event_types);
    if (problem) throw badRequest(problem);
    const { rows } = await pool.query(
      // Any on/off change by a person clears why Reliant turned it off (a 410).
      `UPDATE endpoints SET is_active = CASE WHEN $3 THEN $4 ELSE is_active END,
                            disabled_reason = CASE WHEN $3 THEN NULL ELSE disabled_reason END,
                            disabled_at = CASE WHEN $3 THEN NULL ELSE disabled_at END,
                            event_types = CASE WHEN $5 THEN $6::text[] ELSE event_types END
       WHERE endpoint_id = $1 AND tenant_id = $2 RETURNING endpoint_id, url, is_active, disabled_reason, event_types`,
      [id, t, setActive, setActive ? body.is_active : null, setTypes, setTypes ? body.event_types : null],
    );
    return rows[0];
  }],

  ['POST', /^\/api\/endpoints\/([^/]+)\/rotate-secret$/, async (t, [id]) => {
    await ownEndpoint(t, id);
    const secret = generateSecret();
    await pool.query(
      `UPDATE endpoints SET secret_previous_encrypted = secret_encrypted, secret_encrypted = $3, secret_rotated_at = NOW()
       WHERE endpoint_id = $1 AND tenant_id = $2`,
      [id, t, await encryptSecret(secret)],
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
    const reasons = ['delivery_exhausted', 'payload_rejected', 'processing_error', 'endpoint_unreachable', 'endpoint_gone'];
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
    if (req.method === 'POST' && path === '/api/auth/login') {
      // JSON and same-origin here too: otherwise another site could sign a visitor into the
      // attacker's account (login CSRF).
      if (!sameOriginJson(req)) return sendJson(res, 403, { error: 'cross-site or non-JSON request refused' });
      const body = await readJson(req, 4096);
      const token = await login(body.email, body.password, clientIp(req));
      if (!token) return sendJson(res, 401, { error: 'wrong email or password' });
      return sendJson(res, 200, { signed_in: true }, { 'Set-Cookie': sessionCookie(token) });
    }
    for (const [method, pattern, handler, roles = method === 'GET' ? ANY_ROLE : ['owner']] of routes) {
      const match = path.match(pattern);
      if (!match || req.method !== method) continue;
      const who = await authenticate(req);
      if (!who) return sendJson(res, 401, { error: 'sign in, or send a tenant API key' });
      // A browser sends the cookie with whatever request a page makes; only same-origin JSON
      // may change anything (lib/auth.js). API-key calls carry no cookie, so need no check.
      if (method !== 'GET' && who.via === 'session' && !sameOriginJson(req)) {
        return sendJson(res, 403, { error: 'cross-site or non-JSON request refused' });
      }
      if (!roles.includes(who.role)) return sendJson(res, 403, { error: `a ${who.role} can't do this` });
      const body = method === 'GET' ? {} : await readJson(req);
      const result = await handler(who.tenantId, match.slice(1).map(decodeURIComponent), body, who);
      return Array.isArray(result) && typeof result[0] === 'number'
        ? sendJson(res, result[0], result[1], result[2])
        : sendJson(res, 200, result);
    }
    sendJson(res, 404, { error: 'not found' });
  } catch (err) {
    if (!err.status) log.error('dashboard error', { err, stack: err.stack });
    sendJson(res, err.status || 500, { error: err.status ? err.message : 'internal error' });
  }
});

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  onShutdown(async () => {
    await new Promise((resolve) => server.close(() => resolve()));
    await closeRedis();
    await pool.end();
  });
  serveMetrics(9103);
  server.listen(PORT, () => log.info('dashboard listening', { port: PORT }));
}
