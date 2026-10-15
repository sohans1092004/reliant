// Regressions from the induced-failure drills (production/incident-reports/).
// Needs Postgres running.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import pg from 'pg';
import { execFileSync } from 'node:child_process';
import { pool, closeRedis } from '../lib/infra.js';
import { serveMetrics, client } from '../lib/metrics.js';

after(async () => {
  await closeRedis();
  await pool.end();
});

test('an idle pooled connection killed by Postgres does not crash the process', async () => {
  const { rows: [{ pid }] } = await pool.query('SELECT pg_backend_pid() AS pid'); // now idle in the pool
  const admin = new pg.Client({ host: process.env.DB_HOST, port: process.env.DB_PORT, user: process.env.DB_USER,
    password: process.env.DB_PASSWORD, database: process.env.DB_NAME });
  await admin.connect();
  await admin.query('SELECT pg_terminate_backend($1)', [pid]); // what a Postgres restart does to it
  await admin.end();
  await sleep(200); // the pool emits 'error' here; unhandled, the test process would exit
  const { rows } = await pool.query('SELECT 1 AS ok'); // and the pool recovers with a new connection
  assert.equal(rows[0].ok, 1);
});

// Before: node-redis queued commands while disconnected, and its first connect() never
// settles while Redis is unreachable, so the fail-open rate limiter hung instead.
// In a child process, so this file's own Redis client isn't affected.
test('with Redis unreachable, the rate limiter fails open at once instead of hanging', () => {
  const out = execFileSync(process.execPath, ['--input-type=module', '-e', `
    import { takeToken } from './lib/rate-limit.js';
    const t = Date.now();
    const allowed = await takeToken('resilience-test', 1, 1);
    console.log(JSON.stringify({ allowed, ms: Date.now() - t }));
    process.exit(0);`], { env: { ...process.env, REDIS_URL: 'redis://localhost:1' }, timeout: 10_000 }).toString();
  const { allowed, ms } = JSON.parse(out.trim().split('\n').pop());
  assert.equal(allowed, true);
  assert.ok(ms < 2000, `took ${ms}ms`);
});

test('a failing gauge makes /metrics return 500 instead of crashing', async () => {
  const gauge = new client.Gauge({ name: 'test_failing_gauge', help: 'x', collect() { throw new Error('db down'); } });
  const server = serveMetrics(9195);
  try {
    await new Promise((r) => server.once('listening', r));
    const res = await fetch('http://localhost:9195/metrics');
    assert.equal(res.status, 500);
    assert.equal((await fetch('http://localhost:9195/metrics')).status, 500); // still serving
  } finally {
    server.close();
    client.register.removeSingleMetric(gauge.name);
  }
});
