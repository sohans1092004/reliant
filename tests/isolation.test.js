// Tenant isolation (spec §1.2): tenant Y must not be able to see or touch anything of
// tenant X's through the Dashboard API. Needs Postgres + Redis running.
// Run: npm run test:integration
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { server } from '../dashboard-api.js';
import { pool, closeRedis } from '../lib/infra.js';
import { sha256 } from '../lib/http.js';

const X = { id: 'iso_test_x', key: crypto.randomBytes(16).toString('hex') };
const Y = { id: 'iso_test_y', key: crypto.randomBytes(16).toString('hex') };
let base, xEndpoint, yEndpoint, xEvent;

async function call(tenant, method, path, body) {
  const res = await fetch(base + path, {
    method,
    headers: { ...(tenant && { Authorization: `Bearer ${tenant.key}` }), 'Content-Type': 'application/json' },
    body: body && JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

async function cleanup() {
  const ids = [X.id, Y.id];
  await pool.query('DELETE FROM delivery_ledger WHERE tenant_id = ANY($1)', [ids]);
  await pool.query('DELETE FROM delivery_attempts WHERE endpoint_id IN (SELECT endpoint_id FROM endpoints WHERE tenant_id = ANY($1))', [ids]);
  await pool.query('DELETE FROM events WHERE tenant_id = ANY($1)', [ids]);
  await pool.query('DELETE FROM endpoints WHERE tenant_id = ANY($1)', [ids]);
  await pool.query('DELETE FROM tenants WHERE tenant_id = ANY($1)', [ids]);
}

before(async () => {
  await cleanup();
  for (const t of [X, Y]) {
    await pool.query('INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $1, $2)', [t.id, sha256(t.key)]);
  }
  await new Promise((r) => server.listen(0, r));
  base = `http://localhost:${server.address().port}`;

  xEndpoint = (await call(X, 'POST', '/api/endpoints', { url: 'https://example.com/x' })).body.endpoint_id;
  yEndpoint = (await call(Y, 'POST', '/api/endpoints', { url: 'https://example.com/y' })).body.endpoint_id;
  xEvent = crypto.randomUUID();
  await pool.query(`INSERT INTO events (event_id, tenant_id, type, payload, created_at) VALUES ($1, $2, 't', '{}', NOW())`, [xEvent, X.id]);
  await pool.query(`INSERT INTO delivery_ledger (event_id, endpoint_id, tenant_id, attempt_number, status, error_detail) VALUES ($1, $2, $3, 1, 'failed', 'x-secret-error')`, [xEvent, xEndpoint, X.id]);
  await pool.query(`INSERT INTO delivery_attempts (event_id, endpoint_id, status, dead_reason, terminal_at) VALUES ($1, $2, 'dead', 'delivery_exhausted', NOW())`, [xEvent, xEndpoint]);
});

after(async () => {
  await cleanup();
  server.close();
  await closeRedis();
  await pool.end();
});

test('no key / wrong key is rejected', async () => {
  assert.equal((await call(null, 'GET', '/api/endpoints')).status, 401);
  assert.equal((await call({ key: 'nope' }, 'GET', '/api/endpoints')).status, 401);
});

test('positive control: X can see its own data', async () => {
  assert.ok((await call(X, 'GET', '/api/endpoints')).body.some((e) => e.endpoint_id === xEndpoint));
  assert.equal((await call(X, 'GET', `/api/endpoints/${xEndpoint}/deliveries`)).body.length, 1);
  assert.equal((await call(X, 'GET', `/api/endpoints/${xEndpoint}/deliveries/${xEvent}`)).body.current_state, 'dead_lettered');
  assert.equal((await call(X, 'GET', '/api/dlq')).body.length, 1);
});

test('Y cannot list, read, modify, rotate or replay anything of X', async () => {
  const yList = (await call(Y, 'GET', '/api/endpoints')).body;
  assert.deepEqual(yList.map((e) => e.endpoint_id), [yEndpoint]);
  assert.deepEqual((await call(Y, 'GET', '/api/dlq')).body, []);

  for (const [method, path, body] of [
    ['GET', `/api/endpoints/${xEndpoint}/deliveries`],
    ['GET', `/api/endpoints/${xEndpoint}/deliveries/${xEvent}`],
    ['PATCH', `/api/endpoints/${xEndpoint}`, { is_active: false }],
    ['POST', `/api/endpoints/${xEndpoint}/rotate-secret`],
    ['POST', `/api/deliveries/${xEvent}/${xEndpoint}/replay`],
    ['POST', `/api/deliveries/${xEvent}/${yEndpoint}/replay`], // X's event onto Y's own endpoint
    ['GET', `/api/endpoints/${yEndpoint}/deliveries/${xEvent}`],
  ]) {
    const r = await call(Y, method, path, body);
    assert.equal(r.status, 404, `${method} ${path} -> ${r.status}`);
    assert.ok(!JSON.stringify(r.body).includes('x-secret-error'));
  }

  // And nothing actually changed on X's side.
  const { rows: [ep] } = await pool.query('SELECT is_active, secret_rotated_at FROM endpoints WHERE endpoint_id = $1', [xEndpoint]);
  assert.equal(ep.is_active, true);
  assert.equal(ep.secret_rotated_at, null);
  const { rows: [da] } = await pool.query('SELECT status FROM delivery_attempts WHERE event_id = $1', [xEvent]);
  assert.equal(da.status, 'dead');
});

test('endpoint registration rejects internal addresses when private endpoints are off', async () => {
  if (process.env.ALLOW_PRIVATE_ENDPOINTS === 'true') return; // dev .env allows localhost
  for (const url of ['http://127.0.0.1/x', 'http://169.254.169.254/latest/meta-data', 'http://[::1]/x', 'http://[::ffff:127.0.0.1]/x', 'http://localhost:9998', 'file:///etc/passwd']) {
    assert.equal((await call(X, 'POST', '/api/endpoints', { url })).status, 400, url);
  }
});
