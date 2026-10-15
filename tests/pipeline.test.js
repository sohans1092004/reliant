// End-to-end: real ingest-api.js + consumer.js + delivery-worker.js against real
// Postgres/Redis/Kafka, delivering to a local receiver that verifies every signature.
// Needs docker-compose up and NO other consumer.js / delivery-worker.js running.
// Run: npm run test:integration
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { server as dashboard } from '../dashboard-api.js';
import { Partitioners } from 'kafkajs';
import { pool, redis, closeRedis, kafka, TOPICS } from '../lib/infra.js';
import { sha256 } from '../lib/http.js';
import { verify } from '../core/signing.js';

const INGEST = 'http://localhost:3100';
const TENANTS = ['pipe_ok', 'pipe_flaky', 'pipe_down'];
const keys = Object.fromEntries(TENANTS.map((t) => [t, crypto.randomBytes(16).toString('hex')]));
const secrets = {};      // path -> signing secret
const endpoints = {};    // tenant -> endpoint_id
const received = [];     // { path, eventId, verified, status }
const behaviour = { flakyFailuresLeft: 2, downIsDown: true };
const children = [];
let receiver, dashboardBase;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, what, timeoutMs = 40_000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    const v = await fn();
    if (v) return v;
    await sleep(200);
  }
  throw new Error(`timed out waiting for: ${what}`);
}

async function ingest(tenant, extra = {}, token = process.env.INGEST_TOKEN) {
  const res = await fetch(`${INGEST}/events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ tenant_id: tenant, type: 'order.created', payload: { n: 1 }, ...extra }),
  });
  return { status: res.status, body: await res.json() };
}

async function dash(tenant, method, path, body) {
  const res = await fetch(dashboardBase + path, {
    method, headers: { Authorization: `Bearer ${keys[tenant]}`, 'Content-Type': 'application/json' },
    body: body && JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

const okFor = (path, id) => received.filter((r) => r.path === path && r.eventId === id && r.status === 200);
const hits = (path) => received.filter((r) => r.path === path).length;

async function cleanup() {
  const { rows } = await pool.query('SELECT endpoint_id FROM endpoints WHERE tenant_id = ANY($1)', [TENANTS]);
  for (const { endpoint_id } of rows) await (await redis()).del(`cb:${endpoint_id}`);
  await pool.query('DELETE FROM delivery_ledger WHERE tenant_id = ANY($1)', [TENANTS]);
  await pool.query('DELETE FROM delivery_attempts WHERE endpoint_id IN (SELECT endpoint_id FROM endpoints WHERE tenant_id = ANY($1))', [TENANTS]);
  await pool.query('DELETE FROM events WHERE tenant_id = ANY($1)', [TENANTS]);
  await pool.query('DELETE FROM endpoints WHERE tenant_id = ANY($1)', [TENANTS]);
  await pool.query('DELETE FROM tenants WHERE tenant_id = ANY($1)', [TENANTS]);
}

before(async () => {
  await cleanup();
  for (const t of TENANTS) await pool.query('INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $1, $2)', [t, sha256(keys[t])]);

  // The customer's server: verifies the signature like a real integration would.
  receiver = http.createServer(async (req, res) => {
    let body = '';
    for await (const c of req) body += c;
    const path = req.url;
    const verified = verify([secrets[path]], req.headers['webhook-id'], req.headers['webhook-timestamp'], body, req.headers['webhook-signature'], Math.floor(Date.now() / 1000));
    let status = 200;
    if (path === '/flaky' && behaviour.flakyFailuresLeft > 0) { behaviour.flakyFailuresLeft--; status = 503; }
    if (path === '/down' && behaviour.downIsDown) status = 500;
    received.push({ path, eventId: JSON.parse(body).event_id, verified, status });
    res.writeHead(status); res.end();
  });
  await new Promise((r) => receiver.listen(0, r));
  await new Promise((r) => dashboard.listen(0, r));
  dashboardBase = `http://localhost:${dashboard.address().port}`;

  for (const [tenant, path] of [['pipe_ok', '/ok'], ['pipe_flaky', '/flaky'], ['pipe_down', '/down']]) {
    const r = await dash(tenant, 'POST', '/api/endpoints', { url: `http://localhost:${receiver.address().port}${path}` });
    assert.equal(r.status, 201, JSON.stringify(r.body));
    secrets[path] = r.body.secret;
    endpoints[tenant] = r.body.endpoint_id;
  }

  const env = { ...process.env, RETRY_FAST_MODE: 'true', RETRY_SCHEDULE_MINUTES: '1,1', WORKER_POLL_MS: '300', INGEST_PORT: '3100', ALLOW_PRIVATE_ENDPOINTS: 'true' };
  for (const file of ['ingest-api.js', 'consumer.js', 'delivery-worker.js']) {
    const child = spawn(process.execPath, [file], { env, stdio: ['ignore', 'pipe', 'pipe'] });
    if (process.env.VERBOSE) child.stdout.on('data', (d) => process.stdout.write(`[${file}] ${d}`));
    child.stderr.on('data', (d) => process.stderr.write(`[${file}] ${d}`));
    children.push(child);
  }
  await waitFor(() => fetch(`${INGEST}/health`).then((r) => r.ok, () => false), 'ingest API up');
});

after(async () => {
  for (const c of children) c.kill();
  receiver.close();
  dashboard.close();
  await cleanup();
  await closeRedis();
  await pool.end();
});

test('ingest rejects bad token, bad payload, unknown tenant', async () => {
  assert.equal((await ingest('pipe_ok', {}, 'wrong')).status, 401);
  assert.equal((await ingest('pipe_ok', { payload: 'nope' })).status, 400);
  assert.equal((await ingest('no_such_tenant')).status, 400);
});

test('happy path: delivered once, signature verifies, state is "delivered"', async () => {
  const { status, body } = await ingest('pipe_ok');
  assert.equal(status, 202);
  const [hit] = await waitFor(() => okFor('/ok', body.event_id).length && okFor('/ok', body.event_id), 'delivery to /ok');
  assert.equal(hit.verified, true, 'receiver could not verify the HMAC signature');

  const state = await dash('pipe_ok', 'GET', `/api/endpoints/${endpoints.pipe_ok}/deliveries/${body.event_id}`);
  assert.equal(state.body.current_state, 'delivered');

  // Producer retry with the same event_id: deduplicated, not re-delivered.
  const dup = await ingest('pipe_ok', { event_id: body.event_id });
  assert.deepEqual([dup.status, dup.body.duplicate], [200, true]);
  await sleep(1500);
  assert.equal(okFor('/ok', body.event_id).length, 1);
});

test('poison-pill message is skipped without stalling the partition', async () => {
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  await producer.send({ topic: TOPICS.events, messages: [{ key: 'pipe_ok', value: '{not json' }] });
  await producer.disconnect();
  const { body } = await ingest('pipe_ok'); // same key -> same partition, right behind the garbage
  await waitFor(() => okFor('/ok', body.event_id).length, 'delivery after poison pill');
});

test('retries keep per-endpoint order: e1 (failing, backing off) is delivered before e2', async () => {
  const e1 = (await ingest('pipe_flaky')).body.event_id;
  await waitFor(async () => (await pool.query('SELECT 1 FROM delivery_attempts WHERE event_id = $1', [e1])).rowCount, 'e1 queued');
  const e2 = (await ingest('pipe_flaky')).body.event_id;
  await waitFor(() => okFor('/flaky', e2).length, 'e2 delivered');

  const order = received.filter((r) => r.path === '/flaky' && r.status === 200).map((r) => r.eventId);
  assert.deepEqual(order, [e1, e2]);
  const ledger = (await pool.query('SELECT attempt_number, status FROM delivery_ledger WHERE event_id = $1 ORDER BY id', [e1])).rows;
  assert.deepEqual(ledger, [
    { attempt_number: 1, status: 'failed' },
    { attempt_number: 2, status: 'failed' },
    { attempt_number: 3, status: 'delivered' },
  ]);
  assert.ok(received.filter((r) => r.path === '/flaky').every((r) => r.verified));
});

test('exhausted retries -> dead letter -> replay after the endpoint recovers', async () => {
  const { event_id } = (await ingest('pipe_down')).body;
  const dead = await waitFor(async () => {
    const r = await dash('pipe_down', 'GET', '/api/dlq');
    return r.body.find((d) => d.event_id === event_id);
  }, 'dead letter');
  assert.equal(dead.dead_reason, 'delivery_exhausted');
  assert.equal(dead.attempt_count, 3); // inline + 2 retries with RETRY_SCHEDULE_MINUTES=1,1

  const state = await dash('pipe_down', 'GET', `/api/endpoints/${endpoints.pipe_down}/deliveries/${event_id}`);
  assert.equal(state.body.current_state, 'dead_lettered');
  assert.equal(state.body.latest_attempt.http_status_code, 500);

  behaviour.downIsDown = false;
  const replay = await dash('pipe_down', 'POST', `/api/deliveries/${event_id}/${endpoints.pipe_down}/replay`);
  assert.equal(replay.status, 202);
  await waitFor(() => okFor('/down', event_id).length, 'replayed delivery');
  const after = await dash('pipe_down', 'GET', `/api/endpoints/${endpoints.pipe_down}/deliveries/${event_id}`);
  assert.equal(after.body.current_state, 'delivered');
});

test('circuit breaker opens after 5 consecutive failures and stops hitting the endpoint', async () => {
  behaviour.downIsDown = true;
  // Each dead letter costs 3 failures; two events = 6 >= threshold of 5.
  for (let i = 0; i < 3; i++) await ingest('pipe_down');
  await waitFor(async () => (await (await redis()).get(`cb:${endpoints.pipe_down}`))?.includes('"open"'), 'breaker open');

  const before = hits('/down');
  await ingest('pipe_down');
  await sleep(3000);
  assert.equal(hits('/down'), before, 'endpoint was called while its circuit was open');
  const { rows } = await pool.query(
    `SELECT count(*)::int AS n FROM delivery_attempts WHERE endpoint_id = $1 AND status = 'pending'`, [endpoints.pipe_down]);
  assert.ok(rows[0].n >= 1, 'work should be parked, not lost');
});
