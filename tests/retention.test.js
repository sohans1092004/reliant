// Retention: old finished events are archived then deleted; anything recent or still
// pending is kept. Needs Postgres running.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { runRetention } from '../retention.js';
import { pool } from '../lib/infra.js';

const T = 'ret_test';
const ev = { old: crypto.randomUUID(), oldPending: crypto.randomUUID(), recent: crypto.randomUUID() };
let endpoint, archiveDir;

async function cleanup() {
  await pool.query('DELETE FROM delivery_ledger WHERE tenant_id = $1', [T]);
  await pool.query('DELETE FROM delivery_attempts WHERE endpoint_id IN (SELECT endpoint_id FROM endpoints WHERE tenant_id = $1)', [T]);
  await pool.query('DELETE FROM events WHERE tenant_id = $1', [T]);
  await pool.query('DELETE FROM endpoints WHERE tenant_id = $1', [T]);
  await pool.query('DELETE FROM tenants WHERE tenant_id = $1', [T]);
}

before(async () => {
  await cleanup();
  archiveDir = await fs.mkdtemp(path.join(os.tmpdir(), 'reliant-archive-'));
  await pool.query(`INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $1, $2)`, [T, crypto.randomUUID()]);
  endpoint = (await pool.query(`INSERT INTO endpoints (tenant_id, url) VALUES ($1, 'https://example.com') RETURNING endpoint_id`, [T])).rows[0].endpoint_id;
  for (const [id, age] of [[ev.old, '40 days'], [ev.oldPending, '40 days'], [ev.recent, '1 day']]) {
    await pool.query(`INSERT INTO events (event_id, tenant_id, type, payload, created_at) VALUES ($1, $2, 't', '{"k":1}', NOW() - $3::interval)`, [id, T, age]);
    await pool.query(`INSERT INTO delivery_ledger (event_id, endpoint_id, tenant_id, attempt_number, status) VALUES ($1, $2, $3, 1, 'failed')`, [id, endpoint, T]);
  }
  await pool.query(`INSERT INTO delivery_attempts (event_id, endpoint_id, status) VALUES ($1, $2, 'dead')`, [ev.old, endpoint]);
  await pool.query(`INSERT INTO delivery_attempts (event_id, endpoint_id, status) VALUES ($1, $2, 'pending')`, [ev.oldPending, endpoint]);
});

after(async () => {
  await cleanup();
  await fs.rm(archiveDir, { recursive: true, force: true });
  await pool.end();
});

test('refuses a window shorter than the 30-day replay requirement', async () => {
  await assert.rejects(runRetention({ days: 29 }));
});

test('dry run reports but deletes nothing', async () => {
  const r = await runRetention({ days: 35, tenantLike: T });
  assert.deepEqual(r, { dryRun: true, events: 1 });
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE tenant_id = $1', [T])).rows[0].n, 3);
});

test('apply archives then deletes only old, finished events', async () => {
  const r = await runRetention({ days: 35, apply: true, archiveDir, tenantLike: T });
  assert.equal(r.events, 1);

  const left = (await pool.query('SELECT event_id FROM events WHERE tenant_id = $1', [T])).rows.map((x) => x.event_id).sort();
  assert.deepEqual(left, [ev.oldPending, ev.recent].sort(), 'pending and recent events must survive');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM delivery_ledger WHERE event_id = $1', [ev.old])).rows[0].n, 0);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM delivery_attempts WHERE event_id = $1', [ev.old])).rows[0].n, 0);

  const [line] = zlib.gunzipSync(await fs.readFile(r.files[0])).toString().trim().split('\n');
  const archived = JSON.parse(line);
  assert.equal(archived.event.event_id, ev.old);
  assert.deepEqual(archived.event.payload, { k: 1 });
  assert.equal(archived.ledger.length, 1);
  assert.equal(archived.delivery_attempts[0].status, 'dead');
});

test('ledger partitions: coming months exist; an old empty month is dropped, one still in use is kept', async () => {
  const months = (await pool.query(
    `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'delivery_ledger'::regclass`,
  )).rows.map((r) => r.relname);
  const now = new Date();
  const name = (d) => `delivery_ledger_${d.getUTCFullYear()}_${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
  for (let i = 0; i <= 3; i++) assert.ok(months.includes(name(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i, 1)))));

  // Two months long past the cutoff: one empty, one holding a row of a still-pending event.
  await pool.query(`SELECT create_ledger_partition('2020-01-01'), create_ledger_partition('2020-02-01')`);
  await pool.query(`INSERT INTO delivery_ledger (event_id, endpoint_id, tenant_id, attempt_number, status, attempted_at)
                    VALUES ($1, $2, $3, 1, 'failed', '2020-02-15')`, [ev.oldPending, endpoint, T]);
  try {
    const r = await runRetention({ days: 35, apply: true, archiveDir, tenantLike: T });
    assert.ok(r.droppedPartitions.includes('delivery_ledger_2020_01'));
    assert.ok(!r.droppedPartitions.includes('delivery_ledger_2020_02'), 'dropped a month that still had rows');
    assert.equal((await pool.query(`SELECT to_regclass('delivery_ledger_2020_01') AS t`)).rows[0].t, null);
  } finally {
    await pool.query(`DELETE FROM delivery_ledger WHERE attempted_at < '2020-03-01'`);
    await pool.query('DROP TABLE IF EXISTS delivery_ledger_2020_01, delivery_ledger_2020_02');
  }
});
