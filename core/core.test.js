// Phase 1 exit criterion: the core is correct with zero Kafka/Postgres/HTTP.
// Run: npm test
import { test } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { nextRetryDelay, validateRetrySchedule, DEFAULT_SCHEDULE_MINUTES, parseRetryAfter, MAX_RETRY_AFTER_MS } from './backoff.js';
import * as cb from './circuit-breaker.js';
import * as sig from './signing.js';
import { validateEvent, validateEventTypes } from './event.js';
import { hashPassword, verifyPassword, passwordProblem } from './password.js';

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

test('backoff: tenant schedules are bounded', () => {
  assert.equal(validateRetrySchedule([1, 5, 30]), null);
  assert.equal(validateRetrySchedule([1440]), null);
  for (const bad of [[], [0], [1.5], [1441], ['5'], Array(11).fill(1), null, 'nope']) {
    assert.notEqual(validateRetrySchedule(bad), null, JSON.stringify(bad));
  }
  assert.equal(nextRetryDelay(2, { schedule: [1], jitter: 0 }), null, 'a 1-entry schedule allows exactly 1 retry');
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
  const [newSecret, oldSecret] = [sig.generateSecret(), sig.generateSecret()];
  const ep = { secret: newSecret, secretPrevious: oldSecret, rotatedAt: new Date(0) };
  assert.deepEqual(sig.activeSecrets(ep, 1000), [newSecret, oldSecret]);
  assert.deepEqual(sig.activeSecrets(ep, sig.ROTATION_WINDOW_MS), [newSecret]);
  const h = sig.signatureHeader(sig.activeSecrets(ep, 1000), 'e', 5, 'b');
  assert.ok(sig.verify([oldSecret], 'e', 5, 'b', h, 5), 'receiver still on old secret');
  assert.ok(sig.verify([newSecret], 'e', 5, 'b', h, 5), 'receiver already on new secret');
});

// The published Standard Webhooks test vector: matching it means receivers can verify
// with the official libraries (sdk/python also checks against the library itself).
test('signing: matches the Standard Webhooks test vector; rejects malformed secrets', () => {
  const body = '{"test": 2432232314}';
  assert.equal(sig.sign('whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw', 'msg_p5jXN8AQM9LWM0D4loKWxJek', 1614265330, body),
    'v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=');
  for (const bad of ['whsec_', 'whsec_not base64!', 'whsec_abc']) assert.throws(() => sig.sign(bad, 'e', 1, 'b'), /whsec_<base64>/, bad);
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

test('subscription event types: null = all, else 1-50 valid type names', () => {
  assert.equal(validateEventTypes(null), null);
  assert.equal(validateEventTypes(['order.created', 'invoice.paid']), null);
  for (const bad of [[], 'order.created', [''], ['has space'], [42], Array(51).fill('x')]) {
    assert.ok(validateEventTypes(bad), `accepted ${JSON.stringify(bad)}`);
  }
});

test('password: scrypt hash verifies, salts differ, wrong or tampered fails', async () => {
  const pw = 'correct horse battery';
  const [h1, h2] = [await hashPassword(pw), await hashPassword(pw)];
  assert.match(h1, /^scrypt\$32768\$8\$1\$[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+$/);
  assert.notEqual(h1, h2, 'same password, different salt');
  assert.ok(!h1.includes(pw));
  assert.ok(await verifyPassword(pw, h1));
  assert.ok(!(await verifyPassword('correct horse battery!', h1)));
  assert.ok(!(await verifyPassword(pw, h1.replace(/.$/, (c) => (c === 'A' ? 'B' : 'A')))), 'tampered hash');
  for (const bad of ['', 'plaintext', 'bcrypt$x$y']) assert.ok(!(await verifyPassword(pw, bad)), bad);
});

test('password: policy is length-based and rejects the email name', () => {
  assert.equal(passwordProblem('long enough pass'), null);
  assert.match(passwordProblem('short'), /at least 12/);
  assert.match(passwordProblem('x'.repeat(201)), /at most 200/);
  assert.match(passwordProblem('alice-password-1', 'alice@example.com'), /email name/);
  assert.match(passwordProblem(12345678901234), /string/);
});

test('Retry-After: seconds or HTTP date, capped at a day, junk ignored', () => {
  const now = Date.parse('2026-01-01T00:00:00Z');
  assert.equal(parseRetryAfter('120', now), 120_000);
  assert.equal(parseRetryAfter(' 0 ', now), 0);
  assert.equal(parseRetryAfter('Thu, 01 Jan 2026 00:01:30 GMT', now), 90_000);
  assert.equal(parseRetryAfter('Wed, 31 Dec 2025 23:00:00 GMT', now), 0, 'a date in the past means now');
  assert.equal(parseRetryAfter('999999999', now), MAX_RETRY_AFTER_MS);
  for (const junk of [undefined, null, '', 'soon', '-5', '1.5']) assert.equal(parseRetryAfter(junk, now), null, String(junk));
});
