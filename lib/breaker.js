// Circuit-breaker state in Redis. Logic lives in core/circuit-breaker.js; this is just storage.
// If Redis is wiped, every breaker reads as closed (optimistic) - spec failure scenario #9.
import { redis } from './infra.js';
import * as cb from '../core/circuit-breaker.js';

const key = (endpointId) => `cb:${endpointId}`;

export async function getBreaker(endpointId) {
  const raw = await (await redis()).get(key(endpointId));
  return cb.current(raw ? JSON.parse(raw) : cb.initialState(), Date.now());
}

// ponytail: read-modify-write without a lock - two near-simultaneous outcomes for the
// same endpoint can lose one count. Harmless here (threshold is approximate anyway);
// move to a Lua script if exact counts ever matter.
export async function recordOutcome(endpointId, ok) {
  const before = await getBreaker(endpointId);
  const after = cb.record(before, ok, Date.now());
  await (await redis()).set(key(endpointId), JSON.stringify(after), { EX: 7 * 24 * 3600 });
  if (after.state !== before.state) console.log(`Circuit breaker ${endpointId}: ${before.state} -> ${after.state}`);
  return after;
}

// Batch versions for the consumer: one MGET for many endpoints, and one MULTI that
// writes only the breakers whose state object actually changed.
export async function getBreakers(endpointIds) {
  if (!endpointIds.length) return new Map();
  const raws = await (await redis()).mGet(endpointIds.map(key));
  const now = Date.now();
  return new Map(endpointIds.map((id, i) => [id, cb.current(raws[i] ? JSON.parse(raws[i]) : cb.initialState(), now)]));
}

export async function saveBreakers(changed) { // Map endpointId -> { before, after }
  if (!changed.size) return;
  const multi = (await redis()).multi();
  for (const [id, { before, after }] of changed) {
    multi.set(key(id), JSON.stringify(after), { EX: 7 * 24 * 3600 });
    if (after.state !== before.state) console.log(`Circuit breaker ${id}: ${before.state} -> ${after.state}`);
  }
  await multi.exec();
}

// When an open breaker will next allow a trial delivery.
export const reopensAt = (b) => new Date(b.openedAt + cb.DEFAULT_CONFIG.cooldownMs);
