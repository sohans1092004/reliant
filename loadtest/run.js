// Load tests (spec §14 + v2 §5), scaled to one machine.
//   node loadtest/run.js ingest      ingest throughput ramp
//   node loadtest/run.js isolation   delivery latency, baseline vs. one hanging endpoint
//   node loadtest/run.js recovery    backlog drain after the consumer was offline
//   node loadtest/run.js recovery-spec  the spec benchmark: 600k backlog drained in 5 min?
//   node loadtest/run.js dashboard   dashboard query latency over 1M ledger rows
//   node loadtest/run.js consumer-freeze  zombie consumer: freeze one past the session timeout
//   node loadtest/run.js pg-outage   kill Postgres mid-traffic; was any accepted event lost?
//                        (also redis-outage, kafka-outage)
// Needs docker compose up and NO other ingest/consumer/worker running.
// Uses its own lt_* tenants and deletes everything it created at the end.
import http from 'node:http';
import crypto from 'node:crypto';
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
import { Worker, isMainThread, workerData, parentPort } from 'node:worker_threads';
import { pool, closeRedis, redis, kafka, TOPICS, EVENT_PARTITIONS } from '../lib/infra.js';
import { sha256 } from '../lib/http.js';
import { encryptSecret } from '../lib/infra.js';
import { generateSecret } from '../core/signing.js';

const INGEST = 'http://localhost:3200';
const CHILD_ENV = { ...process.env, INGEST_PORT: '3200', RATE_LIMIT_PER_SEC: '1000000', ALLOW_PRIVATE_ENDPOINTS: 'true' };
const children = {};
const exits = []; // children that exited without being stopped
const ingestPorts = [3200];

// ---------- infrastructure ----------

function start(name, file, env = {}) {
  const child = spawn(process.execPath, [file], { env: { ...CHILD_ENV, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  // LT_LOG_DIR keeps each process's full output (JSON logs) for the incident timeline.
  const logFile = process.env.LT_LOG_DIR && fs.createWriteStream(`${process.env.LT_LOG_DIR}/${name}.log`, { flags: 'a' });
  child.stdout.on('data', (d) => logFile?.write(d));
  child.stderr.on('data', (d) => {
    logFile?.write(d);
    if (/error/i.test(d) && !/coordinator/i.test(d)) process.stderr.write(`[${name}] ${d}`);
  });
  child.on('exit', (code, signal) => {
    if (children[name] === child) exits.push({ name, code, signal, at: Date.now() });
  });
  children[name] = child;
}
function stop(name) {
  const child = children[name];
  delete children[name];
  child?.kill();
}

// Customer endpoints: /ok answers immediately, /hang never answers (worst kind of broken).
let sinkPort;
// LT_TARGET=k8s: deliveries come from pods, which reach this machine as host.docker.internal.
const K8S = process.env.LT_TARGET === 'k8s';
const SINK_HOST = process.env.LT_SINK_HOST || (K8S ? 'host.docker.internal' : 'localhost');
const received = new Map(); // webhook-id -> times received (the outage drills read it)
const duplicateAt = [];     // when each repeat delivery arrived (ms since epoch)
async function startSink() {
  const server = http.createServer((req, res) => {
    req.resume();
    if (req.url === '/hang') return; // let the client time out
    const id = req.headers['webhook-id'];
    if (id && received.has(id)) duplicateAt.push(Date.now());
    if (id) received.set(id, (received.get(id) || 0) + 1);
    req.on('end', () => { res.writeHead(200); res.end(); });
  });
  server.keepAliveTimeout = 60_000;
  await new Promise((r) => server.listen(0, r));
  sinkPort = server.address().port;
  return server;
}

async function setupTenants(n, paths) {
  const tenants = Array.from({ length: n }, (_, i) => `lt_${i}`);
  for (const t of tenants) {
    await pool.query('INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $1, $2)', [t, sha256(crypto.randomUUID())]);
    for (const path of paths) {
      await pool.query('INSERT INTO endpoints (tenant_id, url, secret_encrypted) VALUES ($1, $2, $3)',
        [t, `http://${SINK_HOST}:${sinkPort}${path}`, await encryptSecret(generateSecret())]);
    }
  }
  return tenants;
}

async function cleanup() {
  const like = 'lt\\_%';
  const { rows } = await pool.query('SELECT endpoint_id FROM endpoints WHERE tenant_id LIKE $1', [like]);
  if (rows.length) await (await redis()).del(rows.map((r) => `cb:${r.endpoint_id}`));
  await pool.query('DELETE FROM delivery_ledger WHERE tenant_id LIKE $1', [like]);
  await pool.query('DELETE FROM delivery_attempts WHERE endpoint_id IN (SELECT endpoint_id FROM endpoints WHERE tenant_id LIKE $1)', [like]);
  await pool.query('DELETE FROM events WHERE tenant_id LIKE $1', [like]);
  await pool.query('DELETE FROM endpoints WHERE tenant_id LIKE $1', [like]);
  await pool.query('DELETE FROM tenants WHERE tenant_id LIKE $1', [like]);
}

// Unconsumed messages in the delivery-workers group, summed over partitions.
async function consumerLag() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const end = await admin.fetchTopicOffsets(TOPICS.events);
    const [{ partitions }] = await admin.fetchOffsets({ groupId: 'delivery-workers', topics: [TOPICS.events] });
    return end.reduce((sum, p) => {
      const committed = Number(partitions.find((c) => c.partition === p.partition)?.offset ?? -1);
      return sum + Number(p.offset) - Math.max(committed, 0);
    }, 0);
  } finally {
    await admin.disconnect();
  }
}

// The ingest ramp publishes events with no consumer running, then deletes their
// archive rows. Move the group past them so they aren't left as a backlog.
// Only safe because the harness refused to start if there was lag beforehand.
async function skipOwnBacklog() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    // Explicit end offsets. resetOffsets({ earliest: false }) commits -1, which the
    // consumer (fromBeginning: true) would read as "start from the beginning".
    const end = await admin.fetchTopicOffsets(TOPICS.events);
    await admin.setOffsets({ groupId: 'delivery-workers', topic: TOPICS.events,
      partitions: end.map((p) => ({ partition: p.partition, offset: p.offset })) });
  } finally {
    await admin.disconnect();
  }
}

async function waitFor(fn, what, timeoutMs = 120_000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await fn()) return;
    await sleep(250);
  }
  throw new Error(`timed out: ${what}`);
}

// ---------- load generation ----------

let seq = 0;
async function send(tenants) {
  const started = performance.now();
  const port = ingestPorts[seq % ingestPorts.length];
  const res = await fetch(`http://localhost:${port}/events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${process.env.INGEST_TOKEN}` },
    body: JSON.stringify({ tenant_id: tenants[seq++ % tenants.length], type: 'load.test', payload: { n: seq } }),
  });
  const body = await res.json().catch(() => ({}));
  return { ok: res.status === 202, status: res.status, eventId: body.event_id, ms: performance.now() - started };
}

// Open loop: a fixed `rate` per second for `seconds`, regardless of how slow responses are.
async function openLoop(tenants, rate, seconds) {
  const inflight = new Set(); let sent = 0, errors = 0;
  const tickMs = 10, perTick = rate * tickMs / 1000;
  let carry = 0;
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) {
    carry += perTick;
    for (; carry >= 1; carry--) {
      const p = send(tenants).then((r) => { if (!r.ok) errors++; }, () => errors++).finally(() => inflight.delete(p));
      inflight.add(p); sent++;
    }
    await sleep(tickMs);
  }
  await Promise.all(inflight);
  return { sent, errors };
}

// ---------- measurement (in Postgres, one clock) ----------

const pct = (xs, p) => {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  return Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]);
};

// Accepted -> first delivery attempt STARTED, per (event, endpoint), for endpoints matching `urlLike`.
async function deliveryLatencies(urlLike, withTime = false) {
  const { rows } = await pool.query(
    `SELECT EXTRACT(EPOCH FROM (MIN(l.attempted_at - l.latency_ms * INTERVAL '1 millisecond') - e.created_at)) * 1000 AS ms,
            EXTRACT(EPOCH FROM e.created_at) * 1000 AS created
     FROM delivery_ledger l JOIN events e USING (event_id) JOIN endpoints ep USING (endpoint_id)
     WHERE l.tenant_id LIKE 'lt\\_%' AND ep.url LIKE $1
     GROUP BY l.event_id, l.endpoint_id, e.created_at`,
    [urlLike],
  );
  return withTime ? rows.map((r) => ({ ms: Number(r.ms), created: Number(r.created) })) : rows.map((r) => Number(r.ms));
}

// p95 per 5-second window of event creation time: shows whether a slowdown is a
// startup transient or steady state.
async function timeline(urlLike) {
  const rows = await deliveryLatencies(urlLike, true);
  const t0 = Math.min(...rows.map((r) => r.created));
  const windows = {};
  for (const r of rows) (windows[Math.floor((r.created - t0) / 5000)] ??= []).push(r.ms);
  return Object.entries(windows).map(([w, ms]) => `${w * 5}-${w * 5 + 5}s: p95 ${pct(ms, 95)}ms`).join(' | ');
}

const eventCount = async () => Number((await pool.query(`SELECT count(*) FROM events WHERE tenant_id LIKE 'lt\\_%'`)).rows[0].count);
const deliveredCount = async () => Number((await pool.query(
  `SELECT count(DISTINCT event_id) FROM delivery_ledger WHERE tenant_id LIKE 'lt\\_%' AND status = 'delivered'`)).rows[0].count);

// The consumer needs a few seconds to join the group; send one event and wait for it.
async function warmUp(tenants) {
  await send(tenants);
  await waitFor(async () => (await deliveredCount()) > 0, 'consumer warm-up');
  await pool.query(`DELETE FROM delivery_ledger WHERE tenant_id LIKE 'lt\\_%'`);
}

// ---------- scenarios ----------

// One Node process is single-threaded, and so is one load generator: both cap out near
// one CPU core (~950 events/s measured). LT_INGEST_PROCS runs N stateless ingest
// processes (what replicas would be in Kubernetes), each driven by its own load
// generator thread, so the generator isn't the bottleneck.
async function ingestRamp() {
  const procs = Number(process.env.LT_INGEST_PROCS || 1);
  const tenants = await setupTenants(30, []); // no endpoints: measure ingest alone
  ingestPorts.length = 0;
  for (let i = 0; i < procs; i++) {
    ingestPorts.push(3200 + i);
    start(`ingest${i}`, 'ingest-api.js', { INGEST_PORT: String(3200 + i) });
  }
  for (const port of ingestPorts) {
    await waitFor(() => fetch(`http://localhost:${port}/health`).then((r) => r.ok, () => false), `ingest :${port} up`);
  }
  await threadedLoop(tenants, 8 * procs, 3); // warm-up: connections, JIT, Kafka producer init
  console.log(`\nIngest ramp: ${procs} ingest process(es) + ${procs} generator thread(s), closed loop, 10s per step`);
  console.log('concurrency  events/s   p50ms  p95ms  p99ms  errors');
  for (const c of [1, 16, 64, 128, 256].map((c) => c * procs)) {
    const r = await threadedLoop(tenants, c, 10);
    console.log(`${String(c).padStart(11)}  ${r.rate.toFixed(0).padStart(8)}  ${String(r.p50).padStart(6)}  ${String(r.p95).padStart(5)}  ${String(r.p99).padStart(5)}  ${r.errors}`);
  }
  for (const n of Object.keys(children)) stop(n);
  await sleep(1000);
  await skipOwnBacklog();
}

// Closed loop: `concurrency` senders back to back for `seconds`, split across one
// generator thread per ingest process. Measures max throughput.
async function threadedLoop(tenants, concurrency, seconds) {
  const results = await Promise.all(ingestPorts.map((port) => new Promise((resolve, reject) => {
    const w = new Worker(new URL(import.meta.url), {
      workerData: { tenants, port, concurrency: Math.ceil(concurrency / ingestPorts.length), seconds },
    });
    w.once('message', resolve);
    w.once('error', reject);
  })));
  const lat = results.flatMap((r) => r.lat);
  return { rate: lat.length / seconds, p50: pct(lat, 50), p95: pct(lat, 95), p99: pct(lat, 99), errors: results.reduce((a, r) => a + r.errors, 0) };
}

async function isolation() {
  const RATE = Number(process.env.LT_RATE || 100), SECONDS = Number(process.env.LT_SECONDS || 30);
  const report = async (label) => {
    const lat = await deliveryLatencies('%/ok');
    console.log(`${label.padEnd(28)} healthy deliveries=${lat.length}  p50=${pct(lat, 50)}ms  p95=${pct(lat, 95)}ms  p99=${pct(lat, 99)}ms`);
    console.log(`  timeline: ${await timeline('%/ok')}`);
    return lat;
  };
  start('ingest', 'ingest-api.js');
  start('consumer', 'consumer.js');
  start('worker', 'delivery-worker.js');
  await waitFor(() => fetch(`${INGEST}/health`).then((r) => r.ok, () => false), 'ingest up');
  console.log(`\nEndpoint isolation: ${RATE} events/s for ${SECONDS}s, 3 tenants`);
  const { Partitioners } = await import('kafkajs');
  const partitioner = Partitioners.DefaultPartitioner();
  const partitionMetadata = Array.from({ length: EVENT_PARTITIONS }, (_, partitionId) => ({ partitionId, leader: 0 }));
  console.log('tenant -> partition:', ['lt_0', 'lt_1', 'lt_2']
    .map((t) => `${t}->${partitioner({ topic: TOPICS.events, partitionMetadata, message: { key: t } })}`).join(' '));

  // Baseline: every tenant has two healthy endpoints.
  let tenants = await setupTenants(3, ['/ok', '/ok']);
  await warmUp(tenants);
  await openLoop(tenants, RATE, SECONDS);
  await waitFor(async () => (await deliveredCount()) >= (await eventCount()) - 1, 'baseline drained');
  const base = await report('baseline (2 healthy):');
  await cleanup();

  // Same load, but every tenant also has one endpoint that never answers.
  tenants = await setupTenants(3, ['/ok', '/ok', '/hang']);
  await openLoop(tenants, RATE, SECONDS);
  await waitFor(async () => (await deliveryLatencies('%/ok')).length >= (await eventCount()) * 2, 'healthy endpoints drained', 300_000);
  const bad = await report('with 1 hanging endpoint:');
  const { rows } = await pool.query(`SELECT count(*) FROM delivery_attempts da JOIN endpoints ep USING (endpoint_id) WHERE ep.url LIKE '%/hang' AND ep.tenant_id LIKE 'lt\\_%'`);
  const opens = await Promise.all((await pool.query(`SELECT endpoint_id FROM endpoints WHERE url LIKE '%/hang' AND tenant_id LIKE 'lt\\_%'`)).rows
    .map(async (r) => JSON.parse((await (await redis()).get(`cb:${r.endpoint_id}`)) || '{"state":"closed"}').state));
  console.log(`hanging endpoints: ${rows[0].count} deliveries queued for retry, breakers: ${opens.join(', ')}`);
  console.log(`p95 change: ${pct(base, 95)}ms -> ${pct(bad, 95)}ms`);
}

async function recovery() {
  const RATE = Number(process.env.LT_RATE || 300), SECONDS = Number(process.env.LT_SECONDS || 60);
  const CONSUMERS = Number(process.env.LT_CONSUMERS || 1); // > partition count (3) adds nothing
  const tenants = await setupTenants(30, ['/ok']);
  start('ingest', 'ingest-api.js');
  start('consumer', 'consumer.js');
  await waitFor(() => fetch(`${INGEST}/health`).then((r) => r.ok, () => false), 'ingest up');
  await warmUp(tenants);

  console.log(`\nRecovery: consumer offline while ingesting ${RATE} events/s for ${SECONDS}s, then ${CONSUMERS} consumer(s)`);
  stop('consumer');
  const { sent, errors } = await openLoop(tenants, RATE, SECONDS);
  const backlog = await eventCount() - 1; // minus the warm-up event, whose ledger rows warmUp() deleted
  console.log(`backlog built: ${backlog} events (${sent} sent, ${errors} errors)`);

  const t0 = Date.now();
  for (let i = 0; i < CONSUMERS; i++) start(`consumer${i}`, 'consumer.js');
  let last = 0;
  const progress = setInterval(async () => {
    const d = await deliveredCount();
    console.log(`  t+${Math.round((Date.now() - t0) / 1000)}s delivered ${d}/${backlog} (${Math.round((d - last) / 5)}/s)`);
    last = d;
  }, 5000);
  await waitFor(async () => (await deliveredCount()) >= backlog, 'backlog drained', 900_000);
  clearInterval(progress);
  const secs = (Date.now() - t0) / 1000;
  console.log(`drained ${backlog} events in ${secs.toFixed(1)}s (incl. consumer start + group join) = ${Math.round(backlog / secs)} events/s`);
  console.log(`spec target: 600,000 backlog in 300s = 2,000 events/s`);
}

// Induced failure (spec Phase 4): kill -9 one dependency under steady traffic, restart it
// LT_OUTAGE seconds later, then check end to end, at the receiver, that every event
// ingest accepted was delivered. Fast retry mode, so the drain takes seconds, not minutes.
//
// LT_TARGET=k8s (postgres only) runs it against the deployed cluster instead: load goes to
// the ingest Service (port-forward it to :3200; DB_* must point at a port-forward of the
// postgres Service; env from k8s/local/secrets.env). Postgres is crash-stopped inside its
// pod and Kubernetes restarts the container (emptyDir survives a container restart, not a
// pod deletion); crashes show up as pod restart counts.
const CONTAINERS = { postgres: 'reliant-reliant_postgres-1', redis: 'reliant-reliant_redis-1', redpanda: 'reliant-reliant_kafka-1' };
async function outage(dep) {
  const RATE = Number(process.env.LT_RATE || 50), OUTAGE = Number(process.env.LT_OUTAGE || 30);
  const BEFORE = 15, AFTER = 30;
  const container = CONTAINERS[dep];
  const docker = (...args) => execFileSync('docker', args, { stdio: 'pipe' }).toString().trim();
  const kubectl = (...args) => execFileSync('kubectl', ['-n', 'reliant', ...args], { stdio: 'pipe' }).toString().trim();
  const restarts = () => kubectl('get', 'pods', '-l', 'app.kubernetes.io/name=reliant', '-o',
    'jsonpath={range .items[*]}{.metadata.labels.app\\.kubernetes\\.io/component}={.status.containerStatuses[0].restartCount} {end}');
  if (K8S && dep !== 'postgres') throw new Error('LT_TARGET=k8s supports pg-outage only');
  const t0 = Date.now(), at = () => `t+${((Date.now() - t0) / 1000).toFixed(1)}s`;
  const say = (msg) => console.log(`${at().padEnd(9)} ${msg}`);

  const tenants = await setupTenants(10, ['/ok']);
  const env = { RETRY_FAST_MODE: 'true' };
  if (!K8S) for (const [name, file] of [['ingest', 'ingest-api.js'], ['consumer', 'consumer.js'],
    ['worker', 'delivery-worker.js'], ['retry-publisher', 'retry-publisher.js']]) start(name, file, env);
  const restartsBefore = K8S && restarts();
  await waitFor(() => fetch(`${INGEST}/health`).then((r) => r.ok, () => false), 'ingest up');
  await warmUp(tenants);
  received.clear();

  // Open loop, recording every response. A connection error counts as status 0.
  const responses = []; // { t, status, eventId }
  const inflight = new Set();
  let stopLoad = false;
  const load = (async () => {
    while (!stopLoad) {
      const t = Date.now() - t0;
      const p = send(tenants).catch(() => ({ status: 0 }))
        .then((r) => responses.push({ t, status: r.status, eventId: r.eventId }))
        .finally(() => inflight.delete(p));
      inflight.add(p);
      await sleep(1000 / RATE);
    }
    await Promise.all(inflight);
  })();
  const ticker = setInterval(() => {
    const last = responses.filter((r) => r.t > Date.now() - t0 - 5000);
    const by = Object.entries(Object.groupBy(last, (r) => r.status)).map(([s, rs]) => `${s}:${rs.length}`).join(' ');
    say(`last 5s responses ${by || '-'} | in flight ${inflight.size} | received ${received.size} | alive ${Object.keys(children).filter((n) => !exits.some((e) => e.name === n)).join(',')}`);
  }, 5000);

  console.log(`\n${dep} outage: ${RATE} events/s, kill -9 at t+${BEFORE}s, restart after ${OUTAGE}s`);
  await sleep(BEFORE * 1000);
  if (K8S) {
    // The exec session dies with the container, so kubectl may report an error here.
    try { kubectl('exec', 'deploy/postgres', '--', 'sh', '-c', 'gosu postgres /usr/lib/postgresql/16/bin/pg_ctl stop -m immediate'); } catch {}
    say('postgres crash-stopped in its pod; Kubernetes restarts the container');
    await sleep(OUTAGE * 1000);
  } else {
    docker('kill', container);
    say(`${dep} killed`);
    await sleep(OUTAGE * 1000);
    docker('start', container);
    say(`${dep} started`);
  }
  await sleep(AFTER * 1000);
  stopLoad = true;
  await load;
  clearInterval(ticker);
  say(`load stopped: ${responses.length} requests`);

  // Every event ingest answered 202 for (and 503 "saved, not queued") must reach the receiver.
  const promised = new Set(responses.filter((r) => r.status === 202 || (r.status === 503 && r.eventId)).map((r) => r.eventId));
  const missing = () => [...promised].filter((id) => !received.has(id));
  await waitFor(async () => missing().length === 0, 'all accepted events delivered', 180_000).catch(() => {});
  say('drain finished');

  const status = Object.groupBy(responses, (r) => r.status);
  const firstFail = responses.find((r) => r.status !== 202);
  const lastFail = responses.findLast((r) => r.status !== 202);
  console.log('\n--- result ---');
  console.log('responses by status:', Object.fromEntries(Object.entries(status).map(([s, rs]) => [s, rs.length])));
  if (firstFail) console.log(`non-202 window: t+${(firstFail.t / 1000).toFixed(1)}s .. t+${(lastFail.t / 1000).toFixed(1)}s`);
  console.log(`accepted (202 + 503-saved): ${promised.size}, delivered at receiver: ${[...promised].filter((id) => received.has(id)).length}, NOT DELIVERED within drain timeout: ${missing().length}`);
  console.log(`duplicate deliveries (same webhook-id twice): ${[...received.values()].filter((n) => n > 1).length}`);
  const unexpected = [...received.keys()].filter((id) => !promised.has(id)).length;
  console.log(`delivered but never acknowledged to the client (client saw an error): ${unexpected}`);
  if (K8S) console.log(`pod restarts before: ${restartsBefore}
pod restarts after:  ${restarts()}`);
  else console.log('process exits:', exits.length ? exits.map((e) => `${e.name} code=${e.code} at t+${((e.at - t0) / 1000).toFixed(1)}s`).join('; ') : 'none');
  const { rows } = await pool.query(`SELECT status, count(*)::int AS n FROM delivery_attempts da JOIN endpoints ep USING (endpoint_id)
    WHERE ep.tenant_id LIKE 'lt\\_%' GROUP BY status`);
  console.log('delivery_attempts rows:', Object.fromEntries(rows.map((r) => [r.status, r.n])));
}

// Dashboard query latency (spec v2 §1.2: "what happened to my webhook" p95 < 200ms;
// v1: history p95 < 500ms) against one tenant with 30 days of history: LT_DASH_EVENTS
// events (default 200k) x 5 endpoints = 1M ledger rows, 5% of them still in the retry
// queue. Seeded straight into Postgres; measured through the real dashboard API.
async function dashboard() {
  const EVENTS = Number(process.env.LT_DASH_EVENTS || 200_000), ENDPOINTS = 5;
  const key = crypto.randomUUID();
  await pool.query('INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $1, $2)', ['lt_dash', sha256(key)]);
  const eps = [];
  for (let i = 0; i < ENDPOINTS; i++) {
    eps.push((await pool.query(`INSERT INTO endpoints (tenant_id, url) VALUES ('lt_dash', $1) RETURNING endpoint_id`,
      [`http://localhost/ep${i}`])).rows[0].endpoint_id);
  }
  // The months the history spans need their partitions (normally created as time passes).
  await pool.query(`SELECT create_ledger_partition((NOW() - n * INTERVAL '1 month')::date) FROM generate_series(0, 1) n`);

  console.log(`\nDashboard: seeding ${EVENTS.toLocaleString('en-US')} events x ${ENDPOINTS} endpoints over 30 days...`);
  const t0 = Date.now();
  await pool.query(
    `INSERT INTO events (event_id, tenant_id, type, payload, created_at, published_to_kafka)
     SELECT gen_random_uuid(), 'lt_dash', 'order.created', jsonb_build_object('n', n), NOW() - random() * INTERVAL '30 days', TRUE
     FROM generate_series(1, $1) n`, [EVENTS]);
  await pool.query(
    `INSERT INTO delivery_ledger (event_id, endpoint_id, tenant_id, attempt_number, status, http_status_code, latency_ms, attempted_at)
     SELECT e.event_id, ep, 'lt_dash', 1, 'delivered', 200, 20, e.created_at + INTERVAL '50 milliseconds'
     FROM events e CROSS JOIN unnest($1::uuid[]) ep WHERE e.tenant_id = 'lt_dash'`, [eps]);
  await pool.query(
    `INSERT INTO delivery_attempts (event_id, endpoint_id, status, attempt_count, next_retry_at, created_at)
     SELECT e.event_id, $1, CASE WHEN random() < 0.5 THEN 'pending' ELSE 'dead' END, 2, NOW() + INTERVAL '1 hour', e.created_at
     FROM events e WHERE e.tenant_id = 'lt_dash' AND random() < 0.05`, [eps[0]]);
  await pool.query('ANALYZE events, delivery_ledger, delivery_attempts');
  console.log(`seeded in ${((Date.now() - t0) / 1000).toFixed(0)}s`);

  start('dashboard', 'dashboard-api.js', { DASHBOARD_PORT: '3201', METRICS_PORT: '0' });
  const base = 'http://localhost:3201';
  await waitFor(() => fetch(`${base}/`).then((r) => r.ok, () => false), 'dashboard up');
  const { rows } = await pool.query(`SELECT event_id FROM events WHERE tenant_id = 'lt_dash' ORDER BY random() LIMIT 1000`);
  const get = async (path) => {
    const s = performance.now();
    const res = await fetch(base + path, { headers: { Authorization: `Bearer ${key}` } });
    await res.arrayBuffer();
    if (res.status !== 200) throw new Error(`${path} -> ${res.status}`);
    return performance.now() - s;
  };
  const measure = async (label, target, pathFor, n, concurrency) => {
    const lat = []; let i = 0;
    await Promise.all(Array.from({ length: concurrency }, async () => { while (i < n) lat.push(await get(pathFor(i++))); }));
    const p95 = pct(lat, 95);
    console.log(`${label.padEnd(44)} c=${String(concurrency).padEnd(2)} p50 ${pct(lat, 50)}ms  p95 ${p95}ms  p99 ${pct(lat, 99)}ms  (target p95 < ${target}ms: ${p95 < target ? 'met' : 'NOT met'})`);
  };
  for (let w = 0; w < 50; w++) await get(`/api/endpoints/${eps[0]}/deliveries/${rows[w].event_id}`); // warm-up
  const state = (i) => `/api/endpoints/${eps[i % ENDPOINTS]}/deliveries/${rows[i % rows.length].event_id}`;
  const history = (i) => `/api/endpoints/${eps[i % ENDPOINTS]}/deliveries`;
  for (const c of [1, 8]) {
    await measure('state: "what happened to my webhook"', 200, state, 1000, c);
    await measure('history: last 100 attempts of an endpoint', 500, history, 300, c);
  }
}

// The spec's recovery benchmark (v2 §1.2): consumers offline for 10 minutes at 1,000
// events/s = a 600,000-event backlog, drained within 5 minutes of consumers returning.
// The backlog is written straight to Postgres + Kafka exactly as ingest writes it
// (events row, then the same message keyed by tenant), because this measures the drain,
// not ingest. LT_BACKLOG (default 600000), LT_CONSUMERS (default 6), LT_WORKERS (default 2),
// 30 tenants. Workers are part of delivery capacity: under a backlog the consumer hands
// whatever exceeds an endpoint's per-chunk budget to the retry queue (ADR-007).
async function recoveryBenchmark() {
  const BACKLOG = Number(process.env.LT_BACKLOG || 600_000), CONSUMERS = Number(process.env.LT_CONSUMERS || 6);
  const WORKERS = Number(process.env.LT_WORKERS || 2);
  const tenants = await setupTenants(30, ['/ok']);
  console.log(`\nRecovery benchmark: ${BACKLOG.toLocaleString('en-US')}-event backlog, then ${CONSUMERS} consumer(s) + ${WORKERS} worker(s), ${EVENT_PARTITIONS} partitions`);

  // Built in JS, batch by batch: each batch goes to Postgres, then the same events to Kafka
  // (the order ingest uses), timestamps spread over the 10 "offline" minutes.
  const t0 = Date.now();
  const producer = kafka.producer({ createPartitioner: (await import('kafkajs')).Partitioners.DefaultPartitioner });
  await producer.connect();
  let sent = 0;
  for (let n = 0; n < BACKLOG; n += 20_000) {
    const batch = Array.from({ length: Math.min(20_000, BACKLOG - n) }, (_, i) => ({
      event_id: crypto.randomUUID(), tenant_id: tenants[(n + i) % tenants.length], type: 'load.test',
      payload: { n: n + i }, created_at: new Date(t0 - 600_000 + ((n + i) * 600_000) / BACKLOG).toISOString(),
    }));
    await pool.query(
      `INSERT INTO events (event_id, tenant_id, type, payload, created_at, published_to_kafka)
       SELECT * , TRUE FROM unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::timestamptz[])`,
      ['event_id', 'tenant_id', 'type', 'payload', 'created_at'].map((f) => batch.map((e) => (f === 'payload' ? JSON.stringify(e[f]) : e[f]))));
    await producer.send({ topic: TOPICS.events, messages: batch.map((e) => ({ key: e.tenant_id, value: JSON.stringify(e) })) });
    sent += batch.length;
  }
  await producer.disconnect();
  console.log(`backlog built in ${((Date.now() - t0) / 1000).toFixed(0)}s: ${sent.toLocaleString('en-US')} events in Postgres and Kafka`);

  const start0 = Date.now();
  for (let i = 0; i < CONSUMERS; i++) start(`consumer${i}`, 'consumer.js', { METRICS_PORT: '0' });
  for (let i = 0; i < WORKERS; i++) start(`worker${i}`, 'delivery-worker.js', { METRICS_PORT: '0' });
  let last = 0;
  const progress = setInterval(() => {
    const d = received.size;
    const rate = Math.round((d - last) / 10);
    last = d;
    pool.query(`SELECT count(*)::int AS n FROM delivery_attempts WHERE status = 'pending'`).then(({ rows: [q] }) =>
      console.log(`  t+${Math.round((Date.now() - start0) / 1000)}s received ${d.toLocaleString('en-US')} (${rate}/s), retry queue ${q.n.toLocaleString('en-US')}`));
  }, 10_000);
  await waitFor(async () => received.size >= sent, 'backlog drained', 1_800_000);
  clearInterval(progress);
  const secs = (Date.now() - start0) / 1000;

  // Verified against the ledger, not just the receiver (spec §14).
  const { rows: [l] } = await pool.query(
    `SELECT count(DISTINCT event_id)::int AS delivered FROM delivery_ledger WHERE tenant_id LIKE 'lt\\_%' AND status = 'delivered'`);
  console.log(`drained ${sent.toLocaleString('en-US')} in ${secs.toFixed(0)}s (consumer start + group join included) = ${Math.round(sent / secs)} events/s`);
  console.log(`ledger: ${l.delivered.toLocaleString('en-US')} delivered; duplicates at receiver: ${[...received.values()].filter((n) => n > 1).length}`);
  console.log(`spec: ${BACKLOG.toLocaleString('en-US')} within 300s -> ${secs <= 300 ? 'MET' : 'NOT met'}`);
}

// Spec §9 #6: a consumer is cut off (here: frozen with `docker pause`) long enough for
// the group to give its partitions to another consumer, then comes back as a "zombie"
// still holding a half-done batch. Run with LT_SINK_HOST=host.docker.internal: the
// consumers and the worker run as containers (image reliant:dev) in Redpanda's network
// namespace, so they reach the broker at localhost:9092 and everything else through
// host.docker.internal. Checks: nothing lost, per-endpoint order kept, what the zombie did.
async function consumerFreeze() {
  const RATE = Number(process.env.LT_RATE || 50), FREEZE = Number(process.env.LT_OUTAGE || 45);
  const docker = (...args) => execFileSync('docker', args, { stdio: 'pipe' }).toString().trim();
  const t0 = Date.now(), say = (msg) => console.log(`t+${((Date.now() - t0) / 1000).toFixed(1).padEnd(6)} ${msg}`);
  const names = ['rl-consumer-a', 'rl-consumer-b', 'rl-worker'];
  const run = (name, file) => docker('run', '-d', '--name', name, '--network', 'container:reliant-reliant_kafka-1',
    '-e', 'DB_HOST=host.docker.internal', '-e', `DB_PORT=${process.env.DB_PORT}`, '-e', `DB_USER=${process.env.DB_USER}`,
    '-e', `DB_NAME=${process.env.DB_NAME}`, '-e', 'DB_PASSWORD', '-e', 'SECRET_ENCRYPTION_KEY', // values from this env, not the command line
    '-e', 'REDIS_URL=redis://host.docker.internal:6379', '-e', 'KAFKA_BROKER=localhost:9092',
    '-e', 'ALLOW_PRIVATE_ENDPOINTS=true', '-e', 'RETRY_FAST_MODE=true', '-e', 'METRICS_PORT=0',
    'reliant:dev', 'node', file);
  const tenants = await setupTenants(10, ['/ok']);
  try {
    run('rl-consumer-a', 'consumer.js'); run('rl-consumer-b', 'consumer.js'); run('rl-worker', 'delivery-worker.js');
    start('ingest', 'ingest-api.js');
    await waitFor(() => fetch(`${INGEST}/health`).then((r) => r.ok, () => false), 'ingest up');
    const admin = kafka.admin(); await admin.connect();
    await waitFor(async () => (await admin.describeGroups(['delivery-workers'])).groups[0].members.length === 2, 'both consumers in the group', 90_000);
    await admin.disconnect();
    await warmUp(tenants);
    received.clear();

    console.log(`\nConsumer freeze: ${RATE} events/s, rl-consumer-a frozen for ${FREEZE}s (session timeout 30s)`);
    const responses = [];
    let stopLoad = false;
    const load = (async () => {
      const inflight = new Set();
      while (!stopLoad) {
        const p = send(tenants).catch(() => ({ status: 0 })).then((r) => responses.push(r)).finally(() => inflight.delete(p));
        inflight.add(p);
        await sleep(1000 / RATE);
      }
      await Promise.all(inflight);
    })();
    const ticker = setInterval(() => say(`received ${received.size}`), 5000);
    await sleep(15_000);
    docker('pause', 'rl-consumer-a');
    const frozenAt = Date.now();
    say('rl-consumer-a frozen');
    await sleep(FREEZE * 1000);
    docker('unpause', 'rl-consumer-a');
    const resumedAt = Date.now();
    say('rl-consumer-a resumed (zombie)');
    await sleep(30_000);
    stopLoad = true; await load; clearInterval(ticker);

    const accepted = responses.filter((r) => r.status === 202).map((r) => r.eventId);
    await waitFor(async () => accepted.every((id) => received.has(id)), 'all accepted delivered', 120_000).catch(() => {});
    say('drain finished');

    // Per tenant (= per endpoint), compare delivery order with acceptance order.
    const { rows } = await pool.query(`SELECT event_id, tenant_id FROM events WHERE event_id = ANY($1::uuid[]) ORDER BY created_at`, [accepted]);
    const firstSeen = new Map([...received.keys()].map((id, i) => [id, i]));
    let inversions = 0;
    for (const t of tenants) {
      const seq = rows.filter((r) => r.tenant_id === t).map((r) => firstSeen.get(r.event_id)).filter((i) => i !== undefined);
      for (let i = 1; i < seq.length; i++) if (seq[i] < seq[i - 1]) inversions++;
    }
    const { rows: [led] } = await pool.query(
      `SELECT count(*)::int AS rows, count(DISTINCT event_id)::int AS events FROM delivery_ledger WHERE event_id = ANY($1::uuid[]) AND status = 'delivered'`, [accepted]);
    console.log('\n--- result ---');
    console.log(`accepted ${accepted.length}, delivered ${accepted.filter((id) => received.has(id)).length}, NOT DELIVERED ${accepted.filter((id) => !received.has(id)).length}`);
    console.log(`duplicate deliveries at the receiver: ${[...received.values()].filter((n) => n > 1).length}`);
    const during = duplicateAt.filter((t) => t >= frozenAt && t < resumedAt).length;
    console.log(`  repeats arriving while frozen: ${during}, within 5s after resume: ${duplicateAt.filter((t) => t >= resumedAt && t < resumedAt + 5000).length}, `
      + `later: ${duplicateAt.filter((t) => t >= resumedAt + 5000).length}, before freeze: ${duplicateAt.filter((t) => t < frozenAt).length}`);
    const rel = (t) => ((t - frozenAt) / 1000).toFixed(1);
    if (duplicateAt.length) console.log(`  first/last repeat: ${rel(duplicateAt[0])}s / ${rel(duplicateAt.at(-1))}s after the freeze`);
    console.log(`per-endpoint order inversions (first delivery vs acceptance): ${inversions}`);
    console.log(`ledger: ${led.events} events delivered in ${led.rows} 'delivered' rows`);
    console.log('zombie (rl-consumer-a) after resuming:');
    const logs = spawnSync('docker', ['logs', '--since', `${FREEZE + 40}s`, 'rl-consumer-a']); // errors go to stderr
    const zombie = `${logs.stdout}\n${logs.stderr}`.split('\n')
      .filter((l) => /kafka:|error|rebalanc|stale|not aware/i.test(l)).slice(0, 12);
    for (const l of zombie) console.log(`  ${l.slice(0, 220)}`);
  } finally {
    for (const n of names) try { docker('rm', '-f', n); } catch {}
  }
}

// ---------- main ----------

if (!isMainThread) { // a load-generator thread started by threadedLoop
  ingestPorts.splice(0, ingestPorts.length, workerData.port);
  const end = Date.now() + workerData.seconds * 1000;
  const lat = []; let errors = 0;
  await Promise.all(Array.from({ length: workerData.concurrency }, async () => {
    while (Date.now() < end) {
      const r = await send(workerData.tenants).catch(() => ({ ok: false, ms: 0 }));
      r.ok ? lat.push(r.ms) : errors++;
    }
  }));
  parentPort.postMessage({ lat, errors });
  await pool.end();
  process.exit(0);
}

const scenarios = { ingest: ingestRamp, isolation, recovery, 
  dashboard, 'recovery-spec': recoveryBenchmark, 'consumer-freeze': consumerFreeze, 'pg-outage': () => outage('postgres'), 'redis-outage': () => outage('redis'), 'kafka-outage': () => outage('redpanda') };
const name = process.argv[2];
if (!scenarios[name]) {
  console.log(`usage: node loadtest/run.js ${Object.keys(scenarios).join('|')}`);
  process.exit(1);
}
const lag = K8S ? 0 : await consumerLag(); // the in-cluster broker isn't reachable from here
if (lag > 0) {
  console.error(`delivery-workers has ${lag} unconsumed messages. Run the consumer until it catches up first;`);
  console.error('the load test would otherwise mix them into its measurements.');
  process.exit(1);
}
const sink = await startSink();
await cleanup();
try {
  await scenarios[name]();
} finally {
  for (const n of Object.keys(children)) stop(n);
  sink.close();
  await cleanup();
  await closeRedis();
  await pool.end();
}
