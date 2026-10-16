// Dashboard sign-in: sessions, roles, cross-site request defences, brute-force limits.
// Needs Postgres and Redis (docker compose up).
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { server } from '../dashboard-api.js';
import { pool, redis, closeRedis } from '../lib/infra.js';
import { sha256 } from '../lib/http.js';
import { hashPassword } from '../core/password.js';

const [A, B] = ['auth_a', 'auth_b'];
const PW = 'a long test password';
const users = {
  owner: `owner-${crypto.randomUUID().slice(0, 8)}@a.test`,
  viewer: `viewer-${crypto.randomUUID().slice(0, 8)}@a.test`,
  ownerB: `owner-${crypto.randomUUID().slice(0, 8)}@b.test`,
};
const apiKeyA = crypto.randomBytes(16).toString('hex');
let base, host, endpointB;

async function call(method, path, { cookie, body, headers = {} } = {}) {
  const res = await fetch(base + path, {
    method,
    headers: { ...(body !== undefined && { 'Content-Type': 'application/json' }), ...(cookie && { Cookie: cookie }), ...headers },
    body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
  });
  return { status: res.status, body: await res.json().catch(() => ({})), setCookie: res.headers.get('set-cookie') };
}

// The brute-force limit has its own test at the end; the other tests sign in more often
// than it allows, so they start each sign-in with fresh buckets.
async function resetLoginLimits(email) {
  const r = await redis();
  await r.del(`rl:login:email:${email.toLowerCase()}`);
  for await (const k of r.scanIterator({ MATCH: 'rl:login:ip:*' })) await r.del(k);
}

async function signIn(email, password = PW) {
  await resetLoginLimits(email);
  const r = await call('POST', '/api/auth/login', { body: { email, password } });
  assert.equal(r.status, 200, `login ${email}: ${JSON.stringify(r.body)}`);
  return r.setCookie.split(';')[0]; // "reliant_session=<token>"
}

async function cleanup() {
  await pool.query('DELETE FROM users WHERE tenant_id = ANY($1)', [[A, B]]); // sessions cascade
  await pool.query('DELETE FROM endpoints WHERE tenant_id = ANY($1)', [[A, B]]);
  await pool.query('DELETE FROM tenants WHERE tenant_id = ANY($1)', [[A, B]]);
  for (const email of [...Object.values(users), 'nobody@a.test']) await resetLoginLimits(email);
}

before(async () => {
  await cleanup();
  await pool.query('INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $1, $2), ($3, $3, $4)', [A, sha256(apiKeyA), B, sha256(crypto.randomUUID())]);
  const hash = await hashPassword(PW);
  for (const [email, tenant, role] of [[users.owner, A, 'owner'], [users.viewer, A, 'viewer'], [users.ownerB, B, 'owner']]) {
    await pool.query('INSERT INTO users (tenant_id, email, password_hash, role) VALUES ($1, $2, $3, $4)', [tenant, email, hash, role]);
  }
  endpointB = (await pool.query(`INSERT INTO endpoints (tenant_id, url) VALUES ($1, 'https://example.com/b') RETURNING endpoint_id`, [B])).rows[0].endpoint_id;
  await new Promise((r) => server.listen(0, r));
  host = `localhost:${server.address().port}`;
  base = `http://${host}`;
});

after(async () => {
  server.close();
  await cleanup();
  await closeRedis();
  await pool.end();
});

test('wrong password and unknown email get the same answer', async () => {
  const wrong = await call('POST', '/api/auth/login', { body: { email: users.owner, password: 'not the password' } });
  const unknown = await call('POST', '/api/auth/login', { body: { email: 'nobody@a.test', password: PW } });
  assert.deepEqual([wrong.status, unknown.status], [401, 401]);
  assert.deepEqual(wrong.body, unknown.body);
  assert.equal(wrong.setCookie, null);
});

test('sign-in sets a locked-down cookie; only hashes are stored', async () => {
  await resetLoginLimits(users.owner);
  const r = await call('POST', '/api/auth/login', { body: { email: users.owner.toUpperCase(), password: PW } }); // email is case-insensitive
  assert.equal(r.status, 200);
  for (const flag of ['HttpOnly', 'SameSite=Strict', 'Secure', 'Path=/', 'Max-Age=43200']) assert.ok(r.setCookie.includes(flag), `cookie lacks ${flag}`);
  const token = r.setCookie.split(';')[0].split('=')[1];
  const me = await call('GET', '/api/auth/me', { cookie: `reliant_session=${token}` });
  assert.deepEqual(me.body, { tenant_id: A, role: 'owner', via: 'session', email: users.owner });

  const { rows: [u] } = await pool.query('SELECT password_hash FROM users WHERE email = $1', [users.owner]);
  assert.ok(u.password_hash.startsWith('scrypt$') && !u.password_hash.includes(PW));
  assert.equal((await pool.query('SELECT 1 FROM sessions WHERE token_hash = $1', [token])).rowCount, 0, 'raw token stored');
  assert.equal((await pool.query('SELECT 1 FROM sessions WHERE token_hash = $1', [sha256(token)])).rowCount, 1);
});

test('a viewer can read but not change anything; an owner can', async () => {
  const viewer = await signIn(users.viewer), owner = await signIn(users.owner);
  assert.equal((await call('GET', '/api/endpoints', { cookie: viewer })).status, 200);
  assert.equal((await call('GET', '/api/settings', { cookie: viewer })).status, 200);
  for (const [method, path, body] of [['POST', '/api/endpoints', { url: 'https://example.com/v' }], ['PUT', '/api/settings', { retry_schedule_minutes: [1] }],
    ['GET', '/api/users'], ['POST', '/api/users', { email: 'x@a.test', role: 'owner' }]]) {
    assert.equal((await call(method, path, { cookie: viewer, body })).status, 403, `viewer ${method} ${path}`);
  }
  assert.equal((await call('POST', '/api/endpoints', { cookie: owner, body: { url: 'https://example.com/o' } })).status, 201);
});

test("a session only ever sees its own tenant", async () => {
  const owner = await signIn(users.owner);
  assert.equal((await call('GET', `/api/endpoints/${endpointB}/deliveries`, { cookie: owner })).status, 404);
  assert.equal((await call('PATCH', `/api/endpoints/${endpointB}`, { cookie: owner, body: { is_active: false } })).status, 404);
  const { rows: [b] } = await pool.query('SELECT user_id FROM users WHERE email = $1', [users.ownerB]);
  assert.equal((await call('DELETE', `/api/users/${b.user_id}`, { cookie: owner, body: {} })).status, 404);
  assert.ok(!(await call('GET', '/api/users', { cookie: owner })).body.some((u) => u.email === users.ownerB));
});

test('cookie-authenticated writes must be same-origin JSON; API keys are unaffected', async () => {
  const owner = await signIn(users.owner);
  const asForm = await call('PUT', '/api/settings', { cookie: owner, body: '{"retry_schedule_minutes":[2]}', headers: { 'Content-Type': 'text/plain' } });
  const crossSite = await call('PUT', '/api/settings', { cookie: owner, body: { retry_schedule_minutes: [2] }, headers: { Origin: 'https://evil.example' } });
  assert.deepEqual([asForm.status, crossSite.status], [403, 403]);
  assert.equal((await call('PUT', '/api/settings', { cookie: owner, body: { retry_schedule_minutes: [2] }, headers: { Origin: base } })).status, 200);
  assert.equal((await call('POST', '/api/auth/login', { body: { email: users.owner, password: PW }, headers: { Origin: 'https://evil.example' } })).status, 403, 'login CSRF');
  const viaKey = await call('PUT', '/api/settings', { body: '{"retry_schedule_minutes":null}', headers: { Authorization: `Bearer ${apiKeyA}`, 'Content-Type': 'text/plain' } });
  assert.equal(viaKey.status, 200);
});

test('sign-out and expiry end a session', async () => {
  const owner = await signIn(users.owner);
  const out = await call('POST', '/api/auth/logout', { cookie: owner, body: {} });
  assert.equal(out.status, 200);
  assert.match(out.setCookie, /Max-Age=0/);
  assert.equal((await call('GET', '/api/auth/me', { cookie: owner })).status, 401);

  const again = await signIn(users.owner);
  await pool.query(`UPDATE sessions SET expires_at = NOW() - INTERVAL '1 second' WHERE token_hash = $1`, [sha256(again.split('=')[1])]);
  assert.equal((await call('GET', '/api/auth/me', { cookie: again })).status, 401);
});

test('changing a password checks the old one, applies the policy, and signs out other sessions', async () => {
  const [here, elsewhere] = [await signIn(users.viewer), await signIn(users.viewer)];
  assert.equal((await call('POST', '/api/auth/password', { cookie: here, body: { current_password: 'wrong one entirely', new_password: 'another long password' } })).status, 403);
  assert.equal((await call('POST', '/api/auth/password', { cookie: here, body: { current_password: PW, new_password: 'short' } })).status, 400);
  const ok = await call('POST', '/api/auth/password', { cookie: here, body: { current_password: PW, new_password: 'another long password' } });
  assert.equal(ok.status, 200);
  assert.ok(ok.body.other_sessions_signed_out >= 1); // `elsewhere`, plus any earlier test's viewer session
  assert.equal((await call('GET', '/api/auth/me', { cookie: elsewhere })).status, 401, 'other session survived');
  assert.equal((await call('GET', '/api/auth/me', { cookie: here })).status, 200, 'this session should stay');
  assert.equal((await call('POST', '/api/auth/login', { body: { email: users.viewer, password: PW } })).status, 401, 'old password still works');
  await signIn(users.viewer, 'another long password');
});

test('owners manage users; the last owner can never be removed or demoted', async () => {
  const owner = await signIn(users.owner);
  const created = await call('POST', '/api/users', { cookie: owner, body: { email: `new-${crypto.randomUUID().slice(0, 8)}@a.test`, role: 'viewer' } });
  assert.equal(created.status, 201);
  assert.ok(created.body.password.length >= 20 && !('password_hash' in created.body));
  await signIn(created.body.email, created.body.password);
  assert.equal((await call('POST', '/api/users', { cookie: owner, body: { email: created.body.email, role: 'viewer' } })).status, 409);

  const { rows: [me] } = await pool.query('SELECT user_id FROM users WHERE email = $1', [users.owner]);
  assert.equal((await call('PATCH', `/api/users/${me.user_id}`, { cookie: owner, body: { role: 'viewer' } })).status, 409);
  assert.equal((await call('DELETE', `/api/users/${me.user_id}`, { cookie: owner, body: {} })).status, 409);

  const removed = await call('DELETE', `/api/users/${created.body.user_id}`, { cookie: owner, body: {} });
  assert.equal(removed.status, 200);
  assert.equal((await pool.query('SELECT 1 FROM sessions WHERE user_id = $1', [created.body.user_id])).rowCount, 0, 'sessions survived the user');
});

test('repeated failed logins for one email are cut off with 429', async () => {
  await resetLoginLimits(users.ownerB);
  const statuses = [];
  for (let i = 0; i < 7; i++) statuses.push((await call('POST', '/api/auth/login', { body: { email: users.ownerB, password: 'guess number ' + i } })).status);
  assert.deepEqual(statuses.slice(0, 5), [401, 401, 401, 401, 401]);
  assert.deepEqual(statuses.slice(5), [429, 429]);
  // Even the right password is refused while the email is locked out.
  assert.equal((await call('POST', '/api/auth/login', { body: { email: users.ownerB, password: PW } })).status, 429);
});
