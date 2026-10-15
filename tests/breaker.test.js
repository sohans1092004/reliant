// Circuit-breaker updates are atomic: concurrent outcomes for one endpoint are never
// lost. Needs Redis running.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { applyOutcomes, recordOutcome, getBreaker } from '../lib/breaker.js';
import { redis, closeRedis, pool } from '../lib/infra.js';

const ids = [];
const fresh = () => { const id = `test-${crypto.randomUUID()}`; ids.push(id); return id; };

after(async () => {
  await (await redis()).del(ids.map((id) => `cb:${id}`));
  await closeRedis();
  await pool.end();
});

test('4 simultaneous failures are all counted (threshold is 5)', async () => {
  const id = fresh();
  await Promise.all([1, 2, 3, 4].map(() => recordOutcome(id, false)));
  const b = await getBreaker(id);
  assert.deepEqual([b.state, b.failures], ['closed', 4]);
});

test('the 5th failure opens it, even when reported concurrently with others', async () => {
  const id = fresh();
  await Promise.all(Array.from({ length: 5 }, () => recordOutcome(id, false)));
  assert.equal((await getBreaker(id)).state, 'open');
});

test('a batch of outcomes is applied in order on top of concurrent writes', async () => {
  const id = fresh();
  const now = Date.now();
  await Promise.all([
    applyOutcomes(id, [{ ok: false, at: now }, { ok: false, at: now }]),
    applyOutcomes(id, [{ ok: false, at: now }]),
  ]);
  assert.equal((await getBreaker(id)).failures, 3);
});
