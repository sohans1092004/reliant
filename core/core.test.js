// Phase 1 exit criterion: the core is correct with zero Kafka/Postgres/HTTP.
// Run: npm test
import { test } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { nextRetryDelay, DEFAULT_SCHEDULE_MINUTES } from './backoff.js';
import * as cb from './circuit-breaker.js';
import * as sig from './signing.js';
import { validateEvent } from './event.js';

test('backoff: follows the schedule, within jitter bounds, never negative', () => {
  for (let n = 1; n <= DEFAULT_SCHEDULE_MINUTES.length; n++) {
    const base = DEFAULT_SCHEDULE_MINUTES[n - 1] * 60_000;
    for (const r of [0, 0.5, 0.999999]) {
      const d = nextRetryDelay(n, { random: () => r });
      assert.ok(d >= base * 0.9 && d <= base * 1.1, `attempt ${n}: ${d}`);
    }
  }
  assert.equal(nextRetryDelay(1, { random: () => 0.5 }), 60_000);
});

test('backoff: exhausted after the schedule, rejects nonsense input', () => {
  assert.equal(nextRetryDelay(DEFAULT_SCHEDULE_MINUTES.length + 1), null);
  assert.throws(() => nextRetryDelay(0));
  assert.throws(() => nextRetryDelay(1.5));
  assert.equal(nextRetryDelay(1, { unitMs: 1000, jitter: 0 }), 1000); // fast mode
});

const cfg = { failureThreshold: 3, cooldownMs: 1000, halfOpenSuccesses: 2 };
const feed = (outcomes, start = cb.initialState()) =>
  outcomes.reduce((b, [ok, t]) => cb.record(b, ok, t, cfg), start);

test('breaker: opens only after N consecutive failures', () => {
  assert.equal(feed([[false, 0], [false, 1]]).state, 'closed');
  assert.equal(feed([[false, 0], [false, 1], [true, 2], [false, 3], [false, 4]]).state, 'closed'); // blip resets
  const b = feed([[false, 0], [false, 1], [false, 2]]);
  assert.equal(b.state, 'open');
  assert.equal(b.openedAt, 2);
});

test('breaker: open -> half_open after cooldown -> closed after M successes', () => {
  const open = feed([[false, 0], [false, 0], [false, 0]]);
  assert.equal(cb.current(open, 999, cfg).state, 'open');
  assert.equal(cb.current(open, 1000, cfg).state, 'half_open');
  const one = cb.record(open, true, 1000, cfg);
  assert.equal(one.state, 'half_open');
  assert.equal(cb.record(one, true, 1001, cfg).state, 'closed');
});

test('breaker: any failure in half_open re-opens with a fresh cooldown', () => {
  const open = feed([[false, 0], [false, 0], [false, 0]]);
  const half = cb.record(open, true, 1000, cfg);
  const reopened = cb.record(half, false, 1500, cfg);
  assert.equal(reopened.state, 'open');
  assert.equal(cb.current(reopened, 2400, cfg).state, 'open');
  assert.equal(cb.current(reopened, 2500, cfg).state, 'half_open');
});

test('signing: valid signature verifies; tampering, wrong secret, stale timestamp fail', () => {
  const secret = sig.generateSecret();
  const body = JSON.stringify({ a: 1 });
  const h = sig.signatureHeader([secret], 'evt_1', 1000, body);
  assert.ok(sig.verify([secret], 'evt_1', 1000, body, h, 1000));
  assert.ok(!sig.verify([secret], 'evt_1', 1000, body + ' ', h, 1000), 'tampered body');
  assert.ok(!sig.verify([secret], 'evt_2', 1000, body, h, 1000), 'swapped id');
  assert.ok(!sig.verify([sig.generateSecret()], 'evt_1', 1000, body, h, 1000), 'wrong secret');
  assert.ok(!sig.verify([secret], 'evt_1', 1000, body, h, 1301), 'replayed after tolerance');
  assert.ok(!sig.verify([secret], 'evt_1', 'nope', body, h, 1000), 'garbage timestamp');
});

test('signing: rotation window signs with both secrets, then only the new one', () => {
  const ep = { secret: 'new', secretPrevious: 'old', rotatedAt: new Date(0) };
  assert.deepEqual(sig.activeSecrets(ep, 1000), ['new', 'old']);
  assert.deepEqual(sig.activeSecrets(ep, sig.ROTATION_WINDOW_MS), ['new']);
  const h = sig.signatureHeader(sig.activeSecrets(ep, 1000), 'e', 5, 'b');
  assert.ok(sig.verify(['old'], 'e', 5, 'b', h, 5), 'receiver still on old secret');
  assert.ok(sig.verify(['new'], 'e', 5, 'b', h, 5), 'receiver already on new secret');
});

test('signing: encrypt/decrypt round-trip, tamper detected', () => {
  const key = crypto.randomBytes(32);
  const blob = sig.encrypt('whsec_abc', key);
  assert.equal(sig.decrypt(blob, key), 'whsec_abc');
  const bad = Buffer.from(blob, 'base64'); bad[bad.length - 1] ^= 1;
  assert.throws(() => sig.decrypt(bad.toString('base64'), key));
});

test('event validation', () => {
  const ok = { tenant_id: 'tenant_a', type: 'order.created', payload: { x: 1 } };
  assert.deepEqual(validateEvent(ok), []);
  assert.deepEqual(validateEvent({ ...ok, event_id: crypto.randomUUID() }), []);
  assert.equal(validateEvent({ ...ok, event_id: 'nope' }).length, 1);
  assert.equal(validateEvent({ ...ok, payload: [1] }).length, 1);
  assert.equal(validateEvent({ ...ok, tenant_id: "x'; drop" }).length, 1);
  assert.equal(validateEvent(null).length, 1);
});
