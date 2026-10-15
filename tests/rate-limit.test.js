// Token bucket: bursts up to `burst`, then refills at `rate`/s, shared across callers.
// Needs Redis running.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { takeToken } from '../lib/rate-limit.js';
import { redis, closeRedis, pool } from '../lib/infra.js';

const keys = [];
const fresh = () => { const k = `test-${crypto.randomUUID()}`; keys.push(`rl:${k}`); return k; };

after(async () => {
  await (await redis()).del(keys);
  await closeRedis();
  await pool.end();
});

test('allows exactly `burst` at once, then refuses', async () => {
  const k = fresh();
  const results = await Promise.all(Array.from({ length: 8 }, () => takeToken(k, 1, 5)));
  assert.equal(results.filter(Boolean).length, 5);
});

test('refills at `rate` per second', async () => {
  const k = fresh();
  for (let i = 0; i < 3; i++) await takeToken(k, 10, 3); // drain
  assert.equal(await takeToken(k, 10, 3), false);
  await sleep(250); // 10/s * 0.25s = 2.5 tokens
  const got = (await Promise.all([1, 2, 3].map(() => takeToken(k, 10, 3)))).filter(Boolean).length;
  assert.equal(got, 2);
});
