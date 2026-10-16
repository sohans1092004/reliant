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
// Off the defaults (9100-9102) so the test can't collide with locally running services.
const METRICS_PORTS = { 'ingest-api.js': 9190, 'consumer.js': 9191, 'delivery-worker.js': 9192 };
const TENANTS = ['pipe_ok', 'pipe_flaky', 'pipe_down', 'pipe_picky', 'pipe_burst', 'pipe_custom', 'pipe_herd'];
const keys = Object.fromEntries(TENANTS.map((t) => [t, crypto.randomBytes(16).toString('hex')]));
const secrets = {};      // path -> signing secret
const endpoints = {};    // tenant -> endpoint_id
const received = [];     // { path, eventId, type, verified, status }
const stdout = {};       // file -> everything the child printed
const behaviour = { flakyFailuresLeft: 2, downIsDown: true, burstFailuresLeft: 1, pickyRejectsBad: true };
const herd = { inflight: 0, max: 0 }; // concurrent requests at /herd
const children = [];
let receiver, dashboardBase, env;

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
    const event = JSON.parse(body);
    if (path === '/picky' && event.payload.bad && behaviour.pickyRejectsBad) status = 422; // rejects one kind of event, accepts the rest
    if (path === '/burst' && behaviour.burstFailuresLeft > 0) { behaviour.burstFailuresLeft--; status = 503; }
    if (path === '/never') status = 500;
    if (path === '/herd') { // a just-recovered endpoint: count how many requests overlap
      herd.max = Math.max(herd.max, ++herd.inflight);
      await sleep(30);
      herd.inflight--;
    }
    received.push({ path, eventId: event.event_id, type: event.type, verified, status });
    res.writeHead(status); res.end();
  });
  await new Promise((r) => receiver.listen(0, r));
  await new Promise((r) => dashboard.listen(0, r));
  dashboardBase = `http://localhost:${dashboard.address().port}`;

  for (const [tenant, path] of [['pipe_ok', '/ok'], ['pipe_flaky', '/flaky'], ['pipe_down', '/down'], ['pipe_picky', '/picky'], ['pipe_burst', '/burst'], ['pipe_custom', '/never'], ['pipe_herd', '/herd']]) {
    const r = await dash(tenant, 'POST', '/api/endpoints', { url: `http://localhost:${receiver.address().port}${path}` });
    assert.equal(r.status, 201, JSON.stringify(r.body));
    secrets[path] = r.body.secret;
    endpoints[tenant] = r.body.endpoint_id;
  }

  env = { ...process.env, RETRY_FAST_MODE: 'true', RETRY_SCHEDULE_MINUTES: '1,1', WORKER_POLL_MS: '300', INGEST_PORT: '3100', ALLOW_PRIVATE_ENDPOINTS: 'true' };
  for (const [file, metricsPort] of Object.entries(METRICS_PORTS)) {
    const child = spawn(process.execPath, [file], { env: { ...env, METRICS_PORT: String(metricsPort) }, stdio: ['ignore', 'pipe', 'pipe'] });
    stdout[file] = '';
    child.stdout.on('data', (d) => { stdout[file] += d; if (process.env.VERBOSE) process.stdout.write(`[${file}] ${d}`); });
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

test('subscriptions: an endpoint receives only the event types it subscribed to', async () => {
  const url = `http://localhost:${receiver.address().port}/typed`;
  assert.equal((await dash('pipe_ok', 'POST', '/api/endpoints', { url, event_types: [] })).status, 400);
  const r = await dash('pipe_ok', 'POST', '/api/endpoints', { url, event_types: ['invoice.paid'] });
  assert.equal(r.status, 201, JSON.stringify(r.body));
  secrets['/typed'] = r.body.secret;

  const order = (await ingest('pipe_ok', { type: 'order.created' })).body.event_id;
  const invoice = (await ingest('pipe_ok', { type: 'invoice.paid' })).body.event_id;
  await waitFor(() => okFor('/typed', invoice).length && okFor('/ok', order).length && okFor('/ok', invoice).length,
    'typed endpoint gets invoice.paid; the all-types endpoint gets both');
  await sleep(1000);
  assert.equal(okFor('/typed', order).length, 0, 'order.created reached an endpoint not subscribed to it');

  // null = back to every type; then disable it so later tests aren't affected.
  const patched = await dash('pipe_ok', 'PATCH', `/api/endpoints/${r.body.endpoint_id}`, { event_types: null });
  assert.equal(patched.body.event_types, null);
  await dash('pipe_ok', 'PATCH', `/api/endpoints/${r.body.endpoint_id}`, { is_active: false });
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

test('event the endpoint keeps rejecting is set aside after a healthy ping; next event goes through', async () => {
  const bad = (await ingest('pipe_picky', { payload: { bad: true } })).body.event_id;
  await waitFor(async () => (await pool.query('SELECT 1 FROM delivery_attempts WHERE event_id = $1', [bad])).rowCount, 'bad event queued');
  const good = (await ingest('pipe_picky')).body.event_id;

  await waitFor(() => okFor('/picky', good).length, 'good event delivered after the bad one was set aside');
  const { rows: [row] } = await pool.query('SELECT status, dead_reason, attempt_count FROM delivery_attempts WHERE event_id = $1', [bad]);
  assert.deepEqual(row, { status: 'dead', dead_reason: 'payload_rejected', attempt_count: 3 });

  const picky = received.filter((r) => r.path === '/picky');
  assert.ok(picky.some((r) => r.type === 'reliant.ping' && r.status === 200 && r.verified), 'a signed ping was sent');
  const goodAt = picky.findIndex((r) => r.eventId === good);
  assert.ok(picky.slice(0, goodAt).filter((r) => r.eventId === bad).length === 3, 'good event waited for all 3 attempts on the bad one');

  const dlq = await dash('pipe_picky', 'GET', '/api/dlq');
  assert.equal(dlq.body.find((d) => d.event_id === bad)?.dead_reason, 'payload_rejected');

  // The customer fixes their handler, then bulk-replays everything that was set aside.
  behaviour.pickyRejectsBad = false;
  assert.equal((await dash('pipe_picky', 'POST', `/api/endpoints/${endpoints.pipe_picky}/replay-dead`, { reason: 'nope' })).status, 400);
  const replay = await dash('pipe_picky', 'POST', `/api/endpoints/${endpoints.pipe_picky}/replay-dead`, { reason: 'payload_rejected' });
  assert.deepEqual([replay.status, replay.body.requeued], [202, 1]);
  await waitFor(() => okFor('/picky', bad).length, 'set-aside event delivered after bulk replay');
});

test('a burst queued behind a failure in one batch is delivered in exactly the order it was accepted', async () => {
  const ids = [];
  for (let i = 0; i < 6; i++) ids.push((await ingest('pipe_burst', { payload: { i } })).body.event_id); // sequential: accepted in this order
  await waitFor(() => ids.every((id) => okFor('/burst', id).length), 'whole burst delivered');
  const order = received.filter((r) => r.path === '/burst' && r.status === 200).map((r) => r.eventId);
  assert.deepEqual(order, ids);
});

test("a tenant's own retry schedule replaces the global one", async () => {
  // Global schedule in this test run is 1,1 (2 retries). This tenant allows only 1.
  assert.equal((await dash('pipe_custom', 'PUT', '/api/settings', { retry_schedule_minutes: [1] })).status, 200);
  const { event_id } = (await ingest('pipe_custom')).body;
  const dead = await waitFor(async () => (await dash('pipe_custom', 'GET', '/api/dlq')).body.find((d) => d.event_id === event_id), 'dead letter');
  assert.deepEqual([dead.dead_reason, dead.attempt_count], ['delivery_exhausted', 2]); // inline + 1 retry
});

// Spec §9.12 / §14: an endpoint that was down for a long time comes back with a big
// backlog. Three workers, breaker past its cooldown (half-open): the backlog must arrive
// one request at a time, in order, never as a burst.
test('thundering herd: a recovered endpoint with a 40-event backlog gets one request at a time, in order', async () => {
  const ids = Array.from({ length: 40 }, () => crypto.randomUUID());
  await pool.query(
    `INSERT INTO events (event_id, tenant_id, type, payload, created_at)
     SELECT id, 'pipe_herd', 'order.created', '{}', NOW() FROM unnest($1::uuid[]) AS id`, [ids]);
  await pool.query(
    `INSERT INTO delivery_attempts (event_id, endpoint_id, attempt_count, next_retry_at)
     SELECT id, $2, 3, NOW() FROM unnest($1::uuid[]) WITH ORDINALITY AS t(id, n) ORDER BY n`, [ids, endpoints.pipe_herd]);
  // Opened long ago: reads as half_open, so the first deliveries are trials.
  await (await redis()).set(`cb:${endpoints.pipe_herd}`,
    JSON.stringify({ state: 'open', failures: 0, successes: 0, openedAt: Date.now() - 10 * 60_000 }));

  const extra = [1, 2].map((i) => spawn(process.execPath, ['delivery-worker.js'],
    { env: { ...env, METRICS_PORT: String(9192 + i) }, stdio: 'ignore' }));
  try {
    await waitFor(() => ids.every((id) => okFor('/herd', id).length), 'backlog delivered', 60_000);
  } finally {
    for (const c of extra) c.kill();
  }
  assert.equal(herd.max, 1, `${herd.max} requests hit the recovering endpoint at once`);
  assert.deepEqual(received.filter((r) => r.path === '/herd').map((r) => r.eventId), ids, 'not delivered in order');
  assert.equal(JSON.parse(await (await redis()).get(`cb:${endpoints.pipe_herd}`)).state, 'closed');
});

test('metrics: each process exposes what the tests above did', async () => {
  const scrape = async (file) => (await fetch(`http://localhost:${METRICS_PORTS[file]}/metrics`)).text();
  // A series with no labels, e.g. value(text, 'reliant_open_circuit_breakers').
  const value = (text, series) => {
    const line = text.split('\n').find((l) => l.startsWith(series + ' '));
    return line ? Number(line.split(' ').pop()) : undefined;
  };
  // Sum of every series line starting with `prefix` (so a prefix can match several label sets).
  const sum = (text, prefix) => text.split('\n').filter((l) => l.startsWith(prefix)).reduce((a, l) => a + Number(l.split(' ').pop()), 0);

  const ingest = await scrape('ingest-api.js');
  assert.ok(sum(ingest, 'reliant_ingest_requests_total{status="202"}') > 0, 'accepted events counted');
  assert.ok(sum(ingest, 'reliant_ingest_requests_total{status="401"}') >= 1, 'rejected token counted');

  const consumer = await scrape('consumer.js');
  assert.ok(sum(consumer, 'reliant_delivery_attempts_total{source="inline",outcome="delivered"') > 0);
  assert.ok(sum(consumer, 'reliant_delivery_attempts_total{source="inline",outcome="failed"') > 0);
  assert.ok(value(consumer, 'reliant_delivery_latency_seconds_count') > 0);
  assert.ok(sum(consumer, 'reliant_poison_messages_total') >= 1, 'the poison pill test');
  assert.match(consumer, /reliant_consumer_lag\{partition="\d+"\} \d+/);
  assert.match(consumer, /nodejs_eventloop_lag_seconds/, 'default process metrics');

  const worker = await scrape('delivery-worker.js');
  assert.ok(sum(worker, 'reliant_dead_letters_total{reason="delivery_exhausted"}') >= 1);
  assert.ok(sum(worker, 'reliant_dead_letters_total{reason="payload_rejected"}') >= 1);
  assert.ok(sum(worker, 'reliant_endpoint_pings_total{result="healthy"}') >= 1);
  assert.ok(sum(worker, 'reliant_circuit_breaker_transitions_total{from="closed",to="open"}') >= 1);
  assert.ok(sum(worker, 'reliant_dlq_depth{reason="delivery_exhausted"}') >= 1, 'gauge read from Postgres');
  assert.match(worker, new RegExp(`reliant_circuit_breaker_open\{endpoint_id="${endpoints.pipe_down}",tenant="pipe_down"\} 1`), 'per-endpoint gauge from Redis');
  assert.ok(value(worker, 'reliant_oldest_pending_seconds') >= 0);
  assert.match(worker, /reliant_pending_deliveries \d+/);
});

test('logs: every line is JSON; failures and dead letters carry event, endpoint and tenant ids', () => {
  for (const [file, text] of Object.entries(stdout)) {
    for (const line of text.split('\n').filter(Boolean)) {
      // kafkajs's own log lines are JSON too; anything else means a stray console.log.
      assert.doesNotThrow(() => JSON.parse(line), `${file} printed a non-JSON line: ${line.slice(0, 120)}`);
    }
  }
  const consumerLines = stdout['consumer.js'].split('\n').filter(Boolean).map((l) => JSON.parse(l));
  assert.ok(consumerLines.some((l) => l.service === 'consumer' && l.msg === 'chunk processed'), 'structured consumer logs present');
  const workerLines = stdout['delivery-worker.js'].split('\n').filter(Boolean).map((l) => JSON.parse(l));
  assert.ok(workerLines.some((l) => l.msg === 'dead-lettered' && l.event_id && l.endpoint_id && l.tenant_id), 'dead letters carry their ids');
  assert.ok(consumerLines.some((l) => l.msg === 'inline attempt failed, queued for retry' && l.event_id && l.endpoint_id), 'failed attempts carry their ids');
});
