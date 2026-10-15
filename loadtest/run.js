// Load tests (spec §14 + v2 §5), scaled to one machine.
//   node loadtest/run.js ingest      ingest throughput ramp
//   node loadtest/run.js isolation   delivery latency, baseline vs. one hanging endpoint
//   node loadtest/run.js recovery    backlog drain after the consumer was offline
// Needs docker compose up and NO other ingest/consumer/worker running.
// Uses its own lt_* tenants and deletes everything it created at the end.
import http from 'node:http';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { Worker, isMainThread, workerData, parentPort } from 'node:worker_threads';
import { pool, closeRedis, redis, kafka, TOPICS } from '../lib/infra.js';
import { sha256 } from '../lib/http.js';
import { encryptSecret } from '../lib/infra.js';
import { generateSecret } from '../core/signing.js';

const INGEST = 'http://localhost:3200';
const CHILD_ENV = { ...process.env, INGEST_PORT: '3200', RATE_LIMIT_PER_SEC: '1000000', ALLOW_PRIVATE_ENDPOINTS: 'true' };
const children = {};
const ingestPorts = [3200];

// ---------- infrastructure ----------

function start(name, file, env = {}) {
  const child = spawn(process.execPath, [file], { env: { ...CHILD_ENV, ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  child.stderr.on('data', (d) => /error/i.test(d) && !/coordinator/i.test(d) && process.stderr.write(`[${name}] ${d}`));
  children[name] = child;
}
function stop(name) {
  children[name]?.kill();
  delete children[name];
}

// Customer endpoints: /ok answers immediately, /hang never answers (worst kind of broken).
let sinkPort;
async function startSink() {
  const server = http.createServer((req, res) => {
    req.resume();
    if (req.url === '/hang') return; // let the client time out
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
        [t, `http://localhost:${sinkPort}${path}`, encryptSecret(generateSecret())]);
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
  await res.arrayBuffer();
  return { ok: res.status === 202, ms: performance.now() - started };
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
  const partitionMetadata = [0, 1, 2].map((partitionId) => ({ partitionId, leader: 0 }));
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

const scenarios = { ingest: ingestRamp, isolation, recovery };
const name = process.argv[2];
if (!scenarios[name]) {
  console.log(`usage: node loadtest/run.js ${Object.keys(scenarios).join('|')}`);
  process.exit(1);
}
const lag = await consumerLag();
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
