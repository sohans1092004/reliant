// Load tests (spec §14 + v2 §5), scaled to one machine.
//   node loadtest/run.js ingest      ingest throughput ramp
//   node loadtest/run.js isolation   delivery latency, baseline vs. one hanging endpoint
//   node loadtest/run.js recovery    backlog drain after the consumer was offline
//   node loadtest/run.js recovery-spec  the spec benchmark: 600k backlog drained in 5 min?
//   node loadtest/run.js dashboard   dashboard query latency over 1M ledger rows
//   node loadtest/run.js consumer-freeze  zombie consumer: freeze one past the session timeout
//   node loadtest/run.js pg-outage   kill Postgres mid-traffic; was any accepted event lost?
//                        (also redis-outage, kafka-outage)
//   node loadtest/run.js redis-hang  Redis down while every tenant also has a hanging endpoint
//   node loadtest/run.js lease-double-send  freeze a worker mid-POST past its lease
//   node loadtest/run.js consumer-partition  cut one consumer off the broker; the process keeps running
//   LT_TARGET=k8s: k8s-scale-out (KEDA under live load), k8s-rolling-restart (consumers, 1,000/s)
// Needs docker compose up and NO other ingest/consumer/worker running.
// Uses its own lt_* tenants and deletes everything it created at the end.
import http from 'node:http';
import crypto from 'node:crypto';
import { spawn, spawnSync, execFileSync, execFile } from 'node:child_process';
import { promisify } from 'node:util';
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
const ingestPorts = (process.env.LT_INGEST_PORTS || '3200').split(',').map(Number); // k8s: one port-forward per ingest pod

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
const firstAt = new Map();  // webhook-id -> when it first arrived (ms since epoch)
const duplicateAt = [];     // when each repeat delivery arrived (ms since epoch)
const hangHits = [];        // when each request to /hang arrived
const SLOW_MS = Number(process.env.LT_SLOW_MS || 3000);
async function startSink() {
  const server = http.createServer((req, res) => {
    req.resume();
    if (req.url === '/hang') { hangHits.push(Date.now()); return; } // let the client time out
    const id = req.headers['webhook-id'];
    if (id && received.has(id)) duplicateAt.push(Date.now());
    if (id) received.set(id, (received.get(id) || 0) + 1);
    if (id && !firstAt.has(id)) firstAt.set(id, Date.now());
    req.on('end', async () => {
      if (req.url === '/slow') await sleep(SLOW_MS);
      res.writeHead(200); res.end();
    });
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
// A capped keep-alive pool. fetch opens a new connection whenever every open one is busy, and
// at 1,000/s through kubectl port-forward that left ~18,700 sockets in TIME_WAIT: Windows ran
// out of ephemeral ports and even kubectl couldn't connect. Excess requests now queue here.
const agent = new http.Agent({ keepAlive: true, maxSockets: Number(process.env.LT_SOCKETS || 64) });
function send(tenants) {
  const started = performance.now();
  const port = ingestPorts[seq % ingestPorts.length];
  const body = JSON.stringify({ tenant_id: tenants[seq++ % tenants.length], type: 'load.test', payload: { n: seq } });
  return new Promise((resolve, reject) => {
    const req = http.request({ host: 'localhost', port, path: '/events', method: 'POST', agent, headers: {
      'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body), Authorization: `Bearer ${process.env.INGEST_TOKEN}`,
    } }, (res) => {
      let text = '';
      res.setEncoding('utf8');
      res.on('data', (c) => { text += c; });
      res.on('end', () => {
        let parsed = {};
        try { parsed = JSON.parse(text); } catch {}
        resolve({ ok: res.statusCode === 202, status: res.statusCode, eventId: parsed.event_id, ms: performance.now() - started });
      });
    });
    req.on('error', reject);
    req.end(body);
  });
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
async function outage(dep, { paths = ['/ok'] } = {}) {
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

  const tenants = await setupTenants(10, paths);
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

  const phases = { start: Date.now() };
  console.log(`\n${dep} outage: ${RATE} events/s, kill -9 at t+${BEFORE}s, restart after ${OUTAGE}s${paths.includes('/hang') ? ', every tenant also has a hanging endpoint' : ''}`);
  await sleep(BEFORE * 1000);
  if (K8S) {
    // The exec session dies with the container, so kubectl may report an error here.
    try { kubectl('exec', 'deploy/postgres', '--', 'sh', '-c', 'gosu postgres /usr/lib/postgresql/16/bin/pg_ctl stop -m immediate'); } catch {}
    say('postgres crash-stopped in its pod; Kubernetes restarts the container');
    await sleep(OUTAGE * 1000);
  } else {
    docker('kill', container);
    phases.down = Date.now();
    say(`${dep} killed`);
    await sleep(OUTAGE * 1000);
    docker('start', container);
    phases.up = Date.now();
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
  if (paths.includes('/hang') && phases.down) await hangReport(phases);
}

// With Redis down the circuit breakers can't be read, so they count as closed: how hard does
// Reliant hit the hanging endpoints, and does the healthy endpoint's latency suffer?
async function hangReport({ start, down, up }) {
  const perMin = (from, to) => (hangHits.filter((t) => t >= from && t < to).length / ((to - from) / 60_000)).toFixed(0);
  const end = Date.now();
  console.log(`requests to the hanging endpoints per minute (10 endpoints): before ${perMin(start, down)}, Redis down ${perMin(down, up)}, after ${perMin(up, end)}`);
  const lat = await deliveryLatencies('%/ok', true);
  const p = (from, to) => { const ms = lat.filter((r) => r.created >= from && r.created < to).map((r) => r.ms); return `p50 ${pct(ms, 50)}ms p95 ${pct(ms, 95)}ms p99 ${pct(ms, 99)}ms (n=${ms.length})`; };
  console.log(`healthy endpoint, accepted -> first attempt: before ${p(start, down)}`);
  console.log(`                                            Redis down ${p(down, up)}`);
  console.log(`                                            after ${p(up, end)}`);
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
const docker = (...args) => execFileSync('docker', args, { stdio: 'pipe' }).toString().trim();
// A Reliant process in its own container (image reliant:dev), so it can be paused or cut off.
// It shares the broker container's network: Redpanda advertises localhost:9092.
const runContainer = (name, file, extra = [], { network = 'container:reliant-reliant_kafka-1', broker = 'localhost:9092' } = {}) => docker('run', '-d', '--name', name, '--network', network,
  '-e', 'DB_HOST=host.docker.internal', '-e', `DB_PORT=${process.env.DB_PORT}`, '-e', `DB_USER=${process.env.DB_USER}`,
  '-e', `DB_NAME=${process.env.DB_NAME}`, '-e', 'DB_PASSWORD', '-e', 'SECRET_ENCRYPTION_KEY', // values from this env, not the command line
  '-e', 'REDIS_URL=redis://host.docker.internal:6379', '-e', `KAFKA_BROKER=${broker}`,
  '-e', 'ALLOW_PRIVATE_ENDPOINTS=true', '-e', 'RETRY_FAST_MODE=true', '-e', 'METRICS_PORT=0', ...extra,
  'reliant:dev', 'node', file);

async function consumerFreeze() {
  const RATE = Number(process.env.LT_RATE || 50), FREEZE = Number(process.env.LT_OUTAGE || 45);
  const t0 = Date.now(), say = (msg) => console.log(`t+${((Date.now() - t0) / 1000).toFixed(1).padEnd(6)} ${msg}`);
  const names = ['rl-consumer-a', 'rl-consumer-b', 'rl-worker'];
  const run = runContainer;
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

// A worker freezes mid-POST (a GC pause, a VM stall) for longer than its 60s lease. Another
// worker re-claims the rows and delivers them. Then the first one wakes up and gets its
// answers. Expected: the receiver sees each in-flight event twice (the fence can't un-send a
// POST: that's what webhook-id is for), but every row ends with the NEW owner's outcome.
async function leaseDoubleSend() {
  const N = Number(process.env.LT_ROWS || 20), FREEZE = Number(process.env.LT_OUTAGE || 75);
  const t0 = Date.now(), say = (msg) => console.log(`t+${((Date.now() - t0) / 1000).toFixed(1).padEnd(6)} ${msg}`);
  const names = ['rl-worker-a', 'rl-worker-b'];
  const tenants = await setupTenants(N, ['/slow']); // one endpoint per tenant, so one row each and no per-tenant cap
  const { rows: eps } = await pool.query(`SELECT endpoint_id, tenant_id FROM endpoints WHERE tenant_id = ANY($1)`, [tenants]);
  const ids = eps.map(() => crypto.randomUUID());
  await pool.query(`INSERT INTO events (event_id, tenant_id, type, payload, created_at)
    SELECT id, t, 'lease.drill', '{}', NOW() FROM unnest($1::uuid[], $2::text[]) AS x(id, t)`, [ids, eps.map((e) => e.tenant_id)]);
  try {
    runContainer('rl-worker-a', 'delivery-worker.js', ['-e', 'WORKER_POLL_MS=500']);
    await waitFor(() => docker('logs', 'rl-worker-a').includes('delivery worker started'), 'worker A up', 60_000);
    // Rows go in only now, so A is the only worker that can claim them.
    await pool.query(`INSERT INTO delivery_attempts (event_id, endpoint_id, attempt_count, next_retry_at)
      SELECT id, ep, 1, NOW() FROM unnest($1::uuid[], $2::uuid[]) AS x(id, ep)`, [ids, eps.map((e) => e.endpoint_id)]);
    await waitFor(async () => (await pool.query(`SELECT count(*)::int AS n FROM delivery_attempts WHERE event_id = ANY($1::uuid[]) AND lease IS NOT NULL`, [ids])).rows[0].n === N, 'A claimed every row', 30_000);
    await sleep(500); // its POSTs are in flight: /slow answers after LT_SLOW_MS
    docker('pause', 'rl-worker-a');
    say(`worker A frozen with ${N} POSTs in flight (receiver answers after ${SLOW_MS}ms); lease is 60s`);
    runContainer('rl-worker-b', 'delivery-worker.js', ['-e', 'WORKER_POLL_MS=500']);
    say('worker B started');
    await waitFor(async () => (await pool.query(`SELECT count(*)::int AS n FROM delivery_attempts WHERE event_id = ANY($1::uuid[]) AND status = 'delivered'`, [ids])).rows[0].n === N,
      'B delivered every row', (FREEZE + 30) * 1000);
    say('worker B delivered every row (A\'s leases had expired)');
    const left = FREEZE * 1000 - (Date.now() - t0);
    if (left > 0) await sleep(left);
    docker('unpause', 'rl-worker-a');
    say('worker A resumed');
    await waitFor(() => (docker('logs', 'rl-worker-a').match(/lease lost/g) || []).length >= N, 'A noticed every lost lease', 30_000).catch(() => {});
    await sleep(2000);

    const { rows } = await pool.query(`SELECT status, attempt_count, count(*)::int AS n FROM delivery_attempts WHERE event_id = ANY($1::uuid[]) GROUP BY 1, 2`, [ids]);
    const { rows: [led] } = await pool.query(`SELECT count(*)::int AS rows, count(*) FILTER (WHERE status = 'delivered')::int AS delivered FROM delivery_ledger WHERE event_id = ANY($1::uuid[])`, [ids]);
    const counts = ids.map((id) => received.get(id) || 0);
    console.log('\n--- result ---');
    console.log(`rows: ${N}, received at the endpoint: ${counts.filter((c) => c > 0).length}, received twice: ${counts.filter((c) => c === 2).length}, more than twice: ${counts.filter((c) => c > 2).length}`);
    console.log(`worker A "lease lost" log lines: ${(docker('logs', 'rl-worker-a').match(/lease lost/g) || []).length}`);
    console.log('final delivery_attempts state:', rows.map((r) => `${r.n} x ${r.status} (attempt_count ${r.attempt_count})`).join(', '));
    console.log(`ledger: ${led.rows} attempt rows, ${led.delivered} delivered (both workers' attempts recorded: the POSTs happened)`);
  } finally {
    for (const n of names) {
      if (process.env.LT_LOG_DIR) try { fs.writeFileSync(`${process.env.LT_LOG_DIR}/${n}.log`, spawnSync('docker', ['logs', n]).stdout); } catch {}
      try { docker('rm', '-f', n); } catch {}
    }
  }
}

// A network partition between one consumer and the broker, while that consumer keeps
// running and can still reach Postgres, Redis and the receivers (unlike consumer-freeze,
// where the whole process stops). Each consumer reaches Redpanda's DRILL listener over its
// own network; detaching the broker from rl-kafka-a cuts consumer A off and nothing else.
async function consumerPartition() {
  const RATE = Number(process.env.LT_RATE || 50), CUT = Number(process.env.LT_OUTAGE || 60);
  const t0 = Date.now(), say = (msg) => console.log(`t+${((Date.now() - t0) / 1000).toFixed(1).padEnd(6)} ${msg}`);
  const KAFKA = 'reliant-reliant_kafka-1', ALIAS = 'reliant-kafka-drill';
  const names = ['rl-consumer-a', 'rl-consumer-b'], nets = ['rl-out', 'rl-kafka-a', 'rl-kafka-b'];
  const cleanupNet = () => {
    for (const n of names) try { docker('rm', '-f', n); } catch {}
    for (const n of nets.slice(1)) try { docker('network', 'disconnect', '-f', n, KAFKA); } catch {}
    for (const n of nets) try { docker('network', 'rm', n); } catch {}
  };
  cleanupNet();
  for (const n of nets) docker('network', 'create', n);
  for (const n of nets.slice(1)) docker('network', 'connect', '--alias', ALIAS, n, KAFKA);
  const tenants = await setupTenants(10, ['/ok']);
  const admin = kafka.admin(); await admin.connect();
  const members = async () => (await admin.describeGroups(['delivery-workers'])).groups[0].members.map((m) => m.clientHost).length;
  const memberLog = [];
  try {
    for (const [name, net] of [['rl-consumer-a', 'rl-kafka-a'], ['rl-consumer-b', 'rl-kafka-b']]) {
      runContainer(name, 'consumer.js', [], { network: 'rl-out', broker: `${ALIAS}:29092` });
      docker('network', 'connect', net, name);
    }
    start('ingest', 'ingest-api.js');
    start('worker', 'delivery-worker.js', { RETRY_FAST_MODE: 'true' });
    await waitFor(() => fetch(`${INGEST}/health`).then((r) => r.ok, () => false), 'ingest up');
    await waitFor(async () => (await members()) === 2, 'both consumers in the group', 120_000);
    await warmUp(tenants);
    received.clear();

    console.log(`\nConsumer partition: ${RATE} events/s, rl-consumer-a cut off from the broker for ${CUT}s (session timeout 30s); it can still reach Postgres and the receivers`);
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
    const ticker = setInterval(async () => {
      const m = await members().catch(() => '?');
      memberLog.push(m);
      say(`received ${received.size} | group members ${m}`);
    }, 5000);
    await sleep(15_000);
    docker('network', 'disconnect', 'rl-kafka-a', KAFKA);
    const cutAt = Date.now();
    say('broker detached from rl-kafka-a: consumer A is partitioned');
    await sleep(CUT * 1000);
    docker('network', 'connect', '--alias', ALIAS, 'rl-kafka-a', KAFKA);
    const healedAt = Date.now();
    say('partition healed');
    const rejoined = await waitFor(async () => (await members()) === 2, 'consumer A back in the group', 90_000).then(() => Date.now(), () => null);
    say(rejoined ? `consumer A back in the group after ${((rejoined - healedAt) / 1000).toFixed(1)}s` : 'consumer A did NOT rejoin within 90s');
    await sleep(20_000);
    stopLoad = true; await load; clearInterval(ticker);

    const accepted = responses.filter((r) => r.status === 202).map((r) => r.eventId);
    await waitFor(async () => accepted.every((id) => received.has(id)), 'all accepted delivered', 120_000).catch(() => {});
    say('drain finished');

    const { rows } = await pool.query(`SELECT event_id, tenant_id FROM events WHERE event_id = ANY($1::uuid[]) ORDER BY created_at`, [accepted]);
    const firstSeen = new Map([...received.keys()].map((id, i) => [id, i]));
    let inversions = 0;
    for (const t of tenants) {
      const seq = rows.filter((r) => r.tenant_id === t).map((r) => firstSeen.get(r.event_id)).filter((i) => i !== undefined);
      for (let i = 1; i < seq.length; i++) if (seq[i] < seq[i - 1]) inversions++;
    }
    const logsA = spawnSync('docker', ['logs', 'rl-consumer-a']);
    const linesA = `${logsA.stdout}\n${logsA.stderr}`.split('\n').filter(Boolean).map((l) => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
    const chunksAfter = linesA.filter((l) => l.msg === 'chunk processed' && Date.parse(l.ts) > healedAt).length;
    const chunksDuring = linesA.filter((l) => l.msg === 'chunk processed' && Date.parse(l.ts) > cutAt + 5000 && Date.parse(l.ts) < healedAt).length;
    console.log('\n--- result ---');
    console.log(`accepted ${accepted.length}, delivered ${accepted.filter((id) => received.has(id)).length}, NOT DELIVERED ${accepted.filter((id) => !received.has(id)).length}`);
    console.log(`duplicate deliveries at the receiver: ${[...received.values()].filter((n) => n > 1).length}`);
    const rel = (t) => ((t - cutAt) / 1000).toFixed(1);
    if (duplicateAt.length) console.log(`  repeats arrived ${rel(duplicateAt[0])}s to ${rel(duplicateAt.at(-1))}s after the cut`);
    console.log(`per-endpoint order inversions (first delivery vs acceptance): ${inversions}`);
    console.log(`group members every 5s: ${memberLog.join(' ')}`);
    console.log(`consumer A: chunks processed while cut off (after the first 5s): ${chunksDuring}, after the heal: ${chunksAfter}`);
    for (const l of linesA.filter((x) => x.level === 'error' || /rebalanc|crash|restart|disconnect/i.test(JSON.stringify(x))).slice(0, 8)) console.log(`  A: ${JSON.stringify(l).slice(0, 220)}`);
    if (process.env.LT_LOG_DIR) fs.writeFileSync(`${process.env.LT_LOG_DIR}/rl-consumer-a.log`, `${logsA.stdout}${logsA.stderr}`);
  } finally {
    await admin.disconnect();
    cleanupNet();
  }
}

// ---------- Kubernetes drills (bash loadtest/k8s-drill.sh <name>) ----------

const kubectl = (...args) => execFileSync('kubectl', ['-n', 'reliant', ...args], { stdio: 'pipe' }).toString().trim();
// Async for anything polled during load: the receiver runs on this thread, and a synchronous
// kubectl call blocked it for ~0.5s each time (it showed up as delivery latency).
const kubectlAsync = async (...args) => (await promisify(execFile)('kubectl', ['-n', 'reliant', ...args])).stdout.trim();
const readyConsumers = async () => Number(await kubectlAsync('get', 'deploy', 'consumer', '-o', 'jsonpath={.status.readyReplicas}') || 0);

// Open loop at `rate`/s for `seconds`, split over `threads` generator threads (one thread
// tops out near 950/s). Returns every response: { t (ms since t0), status, eventId }.
async function openLoopThreads(tenants, rate, seconds, threads, t0) {
  const parts = await Promise.all(Array.from({ length: threads }, () => new Promise((resolve, reject) => {
    const w = new Worker(new URL(import.meta.url), { workerData: { mode: 'open', tenants, rate: rate / threads, seconds, t0, ports: ingestPorts } });
    w.once('message', resolve); w.once('error', reject);
  })));
  return parts.flat();
}

// Lost / duplicated / reordered, and accepted -> first attempt latency per 15s window.
async function deliveryReport(responses, tenants, t0) {
  const accepted = responses.filter((r) => r.status === 202).map((r) => r.eventId);
  await waitFor(async () => accepted.every((id) => received.has(id)), 'all accepted delivered', 180_000).catch(() => {});
  const { rows } = await pool.query(`SELECT event_id, tenant_id FROM events WHERE event_id = ANY($1::uuid[]) ORDER BY created_at`, [accepted]);
  const firstSeen = new Map([...received.keys()].map((id, i) => [id, i]));
  // Order only exists between requests where one got its 202 before the other was sent (ingest
  // answers 202 after the Kafka ack, so that one is ahead in Kafka). Concurrent requests for
  // the same tenant have no order, and created_at between them is a coin toss: counting those
  // reported ~9,000 "inversions" that were ties. Per tenant: B is out of order if some A that
  // was acknowledged before B was sent arrived after B.
  let naive = 0;
  for (const t of tenants) {
    const seq = rows.filter((r) => r.tenant_id === t).map((r) => firstSeen.get(r.event_id)).filter((i) => i !== undefined);
    for (let i = 1; i < seq.length; i++) if (seq[i] < seq[i - 1]) naive++;
  }
  const tenantOf = new Map(rows.map((r) => [r.event_id, r.tenant_id]));
  const pairs = [];
  for (const t of tenants) {
    const evs = responses.filter((r) => r.status === 202 && tenantOf.get(r.eventId) === t && firstSeen.has(r.eventId));
    const byDone = [...evs].sort((a, b) => (a.t + a.ms) - (b.t + b.ms));
    let j = 0, latest = null; // among events acknowledged before B was sent: the one that arrived last
    for (const b of [...evs].sort((x, y) => x.t - y.t)) {
      for (; j < byDone.length && byDone[j].t + byDone[j].ms < b.t; j++) {
        if (!latest || firstSeen.get(byDone[j].eventId) > firstSeen.get(latest.eventId)) latest = byDone[j];
      }
      if (latest && firstSeen.get(latest.eventId) > firstSeen.get(b.eventId)) pairs.push([latest.eventId, b.eventId]);
    }
  }
  const inversions = pairs.length;
  if (pairs.length) await explainInversions(pairs, t0);
  const { rows: queued } = await pool.query(
    `SELECT COALESCE(split_part(last_error, ' after', 1), 'none (queued behind another, or out of time budget)') AS why, count(*)::int AS n
     FROM delivery_attempts WHERE event_id = ANY($1::uuid[]) GROUP BY 1 ORDER BY 2 DESC LIMIT 5`, [accepted]);
  const status = Object.fromEntries(Object.entries(Object.groupBy(responses, (r) => r.status)).map(([k, v]) => [k, v.length]));
  const lastT = Math.max(...responses.map((r) => r.t));
  console.log('\n--- result ---');
  const ms = responses.filter((r) => r.ms != null).map((r) => r.ms);
  console.log(`ingest responses: ${JSON.stringify(status)}, ${(responses.length / (lastT / 1000)).toFixed(0)} req/s achieved; `
    + `response time (through kubectl port-forward) p50 ${pct(ms, 50)}ms p95 ${pct(ms, 95)}ms max ${pct(ms, 100)}ms`);
  const missing = accepted.filter((id) => !received.has(id));
  const { rows: [still] } = await pool.query(`SELECT count(*)::int AS n FROM delivery_attempts WHERE event_id = ANY($1::uuid[]) AND status = 'pending'`, [missing]);
  console.log(`accepted ${accepted.length}, delivered ${accepted.length - missing.length}, not delivered within the drain timeout ${missing.length}`
    + (missing.length ? ` (${still.n} still queued for retry, ${missing.length - still.n} not queued anywhere = LOST)` : ''));
  console.log(`duplicate deliveries at the receiver: ${[...received.values()].filter((n) => n > 1).length}`);
  const { rows: [rtt] } = await pool.query(`SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) AS p50, percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95
    FROM delivery_ledger WHERE event_id = ANY($1::uuid[])`, [accepted]);
  console.log(`receiver round trip (ledger latency_ms): p50 ${Math.round(rtt.p50)}ms p95 ${Math.round(rtt.p95)}ms -> one endpoint takes at most ~${Math.round(1000 / Math.max(rtt.p50, 1))} events/s in order`);
  console.log(`per-endpoint order inversions: ${inversions} (events delivered before one that was acknowledged before they were sent)`);
  console.log(`  by created_at instead, which also counts concurrent requests that have no order: ${naive}`);
  console.log(`went through the retry queue: ${queued.reduce((a, r) => a + r.n, 0)}; why (last error): ${queued.map((r) => `${r.why} ${r.n}`).join(', ')}`);
  const lat = await deliveryLatencies('%/ok', true);
  const windows = {};
  for (const r of lat) (windows[Math.floor((r.created - t0) / 15_000)] ??= []).push(r.ms);
  console.log('accepted -> first attempt, by 15s window of acceptance time:');
  for (const [w, ms] of Object.entries(windows)) console.log(`  t+${w * 15}-${w * 15 + 15}s: p50 ${pct(ms, 50)}ms p95 ${pct(ms, 95)}ms max ${pct(ms, 100)}ms (n=${ms.length})`);
}

// How each side of an out-of-order pair was delivered: inline (ledger attempt 1 with no
// delivery_attempts row) or through the retry queue, and when.
async function explainInversions(pairs, t0) {
  const ids = [...new Set(pairs.slice(0, 5).flat())];
  const { rows: ev } = await pool.query('SELECT event_id, created_at FROM events WHERE event_id = ANY($1::uuid[])', [ids]);
  const { rows: da } = await pool.query('SELECT event_id, status, attempt_count, created_at, last_error FROM delivery_attempts WHERE event_id = ANY($1::uuid[])', [ids]);
  const { rows: led } = await pool.query(`SELECT event_id, attempt_number, status, error_detail,
      attempted_at - latency_ms * INTERVAL '1 millisecond' AS started FROM delivery_ledger WHERE event_id = ANY($1::uuid[]) ORDER BY id`, [ids]);
  const rel = (d) => `t+${((new Date(d).getTime() - t0) / 1000).toFixed(2)}s`;
  const describe = (id) => {
    const e = ev.find((r) => r.event_id === id), q = da.find((r) => r.event_id === id);
    const attempts = led.filter((r) => r.event_id === id).map((r) => `#${r.attempt_number} ${r.status}${r.error_detail ? ` (${r.error_detail})` : ''} started ${rel(r.started)}`);
    return `accepted ${rel(e.created_at)}, first arrived ${rel(firstAt.get(id))}, `
      + `${q ? `queued at ${rel(q.created_at)} (now ${q.status}, ${q.attempt_count} attempts${q.last_error ? `, ${q.last_error}` : ''})` : 'never queued'}; ledger: ${attempts.join('; ') || 'none'}`;
  };
  console.log('first out-of-order pairs (A accepted first, B delivered first):');
  for (const [a, b] of pairs.slice(0, 5)) console.log(`  A ${a.slice(0, 8)}: ${describe(a)}\n  B ${b.slice(0, 8)}: ${describe(b)}`);
  const { rows: [counts] } = await pool.query(`SELECT count(*)::int AS queued FROM delivery_attempts WHERE event_id = ANY($1::uuid[])`, [pairs.map((p) => p[0])]);
  console.log(`of ${pairs.length} pairs, the earlier event went through the retry queue in ${counts.queued}`);
}

// KEDA under live load (ADR-006 only measured a backlog with no traffic). Each consumer is
// capped at LT_CPU so one replica can't keep up and KEDA has to add more mid-stream.
// Rebalances move partitions while chunks are in flight: does anything get lost,
// duplicated or reordered, and what does it do to latency?
async function k8sScaleOut() {
  if (!K8S) throw new Error('run via: bash loadtest/k8s-drill.sh k8s-scale-out');
  const RATE = Number(process.env.LT_RATE || 200), SECONDS = Number(process.env.LT_SECONDS || 120), CPU = process.env.LT_CPU || '50m';
  const t0 = Date.now(), say = (msg) => console.log(`t+${((Date.now() - t0) / 1000).toFixed(1).padEnd(6)} ${msg}`);
  const tenants = await setupTenants(10, ['/ok']);
  const request = kubectl('get', 'deploy', 'consumer', '-o', 'jsonpath={.spec.template.spec.containers[0].resources.requests.cpu}');
  try {
    kubectl('patch', 'deploy', 'consumer', '--type', 'json', '-p', JSON.stringify([
      { op: 'add', path: '/spec/template/spec/containers/0/resources/limits/cpu', value: CPU },
      { op: 'replace', path: '/spec/template/spec/containers/0/resources/requests/cpu', value: CPU }, // request <= limit
    ]));
    kubectl('rollout', 'status', 'deploy/consumer', '--timeout=180s');
    await waitFor(async () => (await readyConsumers()) === 1, 'one consumer', 240_000); // the 120s scale-down window from any earlier run
    await warmUp(tenants);
    received.clear();
    say(`consumers capped at ${CPU} CPU each; ${RATE} events/s for ${SECONDS}s`);
    const timeline = [];
    // What KEDA sees (the external metric it feeds the HPA) next to the broker's own lag figure.
    const kedaLag = async () => { try { return JSON.parse((await promisify(execFile)('kubectl', ['get', '--raw', '/apis/external.metrics.k8s.io/v1beta1/namespaces/reliant/s0-kafka-webhooks-events?labelSelector=scaledobject.keda.sh%2Fname%3Dconsumer'])).stdout).items[0]?.value; } catch { return '?'; } };
    const brokerLag = async () => { try { return (await kubectlAsync('exec', 'deploy/redpanda', '--', 'rpk', 'group', 'describe', 'delivery-workers')).match(/TOTAL-LAG\s+(\d+)/)?.[1]; } catch { return '?'; } };
    const ticker = setInterval(async () => {
      const [n, b, k] = await Promise.all([readyConsumers(), brokerLag(), kedaLag()]);
      timeline.push(n);
      say(`ready consumers ${n} | received ${received.size} | broker lag ${b} | KEDA metric ${k}`);
    }, 5000);
    const responses = await openLoopThreads(tenants, RATE, SECONDS, 1, t0);
    say('load stopped');
    await deliveryReport(responses, tenants, t0);
    clearInterval(ticker);
    console.log(`ready consumers every 5s: ${timeline.join(' ')}`);
    const first = timeline.findIndex((n) => n > 1);
    console.log(`max: ${Math.max(...timeline)}; ${first < 0 ? 'never scaled up' : `first scale-up seen ${first * 5}-${first * 5 + 5}s into the load`}`);
  } finally {
    // The reverse patch, not `kubectl apply`: apply leaves alone a field it never set (the limit).
    say(`restoring the consumer's CPU: request ${request}, no limit`);
    kubectl('patch', 'deploy', 'consumer', '--type', 'json', '-p', JSON.stringify([
      { op: 'remove', path: '/spec/template/spec/containers/0/resources/limits/cpu' },
      { op: 'replace', path: '/spec/template/spec/containers/0/resources/requests/cpu', value: request },
    ]));
    kubectl('rollout', 'status', 'deploy/consumer', '--timeout=180s');
  }
}

// `kubectl rollout restart` of the consumers at 1,000 events/s: each old pod gets SIGTERM,
// finishes its chunk, commits, and leaves; its partitions move to the others.
async function k8sRollingRestart() {
  if (!K8S) throw new Error('run via: bash loadtest/k8s-drill.sh k8s-rolling-restart');
  const RATE = Number(process.env.LT_RATE || 1000), SECONDS = Number(process.env.LT_SECONDS || 90), REPLICAS = Number(process.env.LT_REPLICAS || 4);
  const t0 = Date.now(), say = (msg) => console.log(`t+${((Date.now() - t0) / 1000).toFixed(1).padEnd(6)} ${msg}`);
  const tenants = await setupTenants(Number(process.env.LT_TENANTS || 100), ['/ok']); // 1,000/s over 100 endpoints = 10/s each
  const setMin = (n) => kubectl('patch', 'scaledobject', 'consumer', '--type', 'merge', '-p', JSON.stringify({ spec: { minReplicaCount: n } }));
  try {
    setMin(REPLICAS);
    await waitFor(async () => (await readyConsumers()) >= REPLICAS, `${REPLICAS} consumers`, 180_000);
    await warmUp(tenants);
    received.clear();
    say(`${await readyConsumers()} consumers; ${RATE} events/s to ${tenants.length} endpoints for ${SECONDS}s over ${ingestPorts.length} ingest pods; rollout restart at t+20s`);
    const ticker = setInterval(async () => say(`ready consumers ${await readyConsumers()} | received ${received.size}`), 5000);
    const restart = (async () => {
      await sleep(20_000);
      await kubectlAsync('rollout', 'restart', 'deploy/consumer');
      say('kubectl rollout restart deploy/consumer');
      await kubectlAsync('rollout', 'status', 'deploy/consumer', '--timeout=300s');
      say('rollout finished');
    })();
    const responses = await openLoopThreads(tenants, RATE, SECONDS, Math.max(2, Math.ceil(RATE / 500)), t0);
    say('load stopped');
    await restart;
    await deliveryReport(responses, tenants, t0);
    clearInterval(ticker);
  } finally {
    setMin(1);
  }
}

// ---------- main ----------

if (!isMainThread && workerData.mode === 'open') { // a generator thread started by openLoopThreads
  ingestPorts.splice(0, ingestPorts.length, ...workerData.ports);
  const { tenants, rate, seconds, t0 } = workerData;
  const responses = [], inflight = new Set();
  // Paced by elapsed time, not per tick: Windows timers fire every ~15.6ms, not every 10ms,
  // so "rate x 10ms per tick" sent 40% too little.
  const start = Date.now(), end = start + seconds * 1000;
  let sent = 0;
  while (Date.now() < end) {
    for (const due = rate * (Date.now() - start) / 1000; sent < due; sent++) {
      const t = Date.now() - t0;
      const p = send(tenants).then((r) => responses.push({ t, status: r.status, eventId: r.eventId, ms: r.ms }), () => responses.push({ t, status: 0 }))
        .finally(() => inflight.delete(p));
      inflight.add(p);
    }
    await sleep(5);
  }
  await Promise.all(inflight);
  parentPort.postMessage(responses);
  await pool.end();
  process.exit(0);
}

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
  dashboard, 'recovery-spec': recoveryBenchmark, 'consumer-freeze': consumerFreeze, 'pg-outage': () => outage('postgres'), 'redis-outage': () => outage('redis'), 'kafka-outage': () => outage('redpanda'),
  'redis-hang': () => outage('redis', { paths: ['/ok', '/hang'] }), 'lease-double-send': leaseDoubleSend,
  'consumer-partition': consumerPartition, 'k8s-scale-out': k8sScaleOut, 'k8s-rolling-restart': k8sRollingRestart };
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
