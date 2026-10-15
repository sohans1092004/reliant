// Circuit-breaker state in Redis. Logic lives in core/circuit-breaker.js; this is just storage.
// If Redis is wiped, every breaker reads as closed (optimistic) - spec failure scenario #9.
import { redis } from './infra.js';
import * as cb from '../core/circuit-breaker.js';

const key = (endpointId) => `cb:${endpointId}`;
const TTL_SECONDS = 7 * 24 * 3600;
const MAX_CAS_RETRIES = 20;

// Compare-and-set: write the new state only if the stored value is still exactly what
// we read. Lets the state machine stay in JS (core/) while updates stay atomic.
const CAS_SCRIPT = `
local cur = redis.call('GET', KEYS[1])
if (cur or '') ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1`;

const parse = (raw) => (raw ? JSON.parse(raw) : cb.initialState());

export async function getBreaker(endpointId) {
  const raw = await (await redis()).get(key(endpointId));
  return cb.current(parse(raw), Date.now());
}

// Apply a sequence of outcomes ({ ok, at }) atomically. Several processes (consumer,
// workers, pings) report outcomes for the same endpoint at once; a plain
// read-modify-write lost counts when they collided. On a conflict, re-read and
// re-apply the same outcomes on top of whatever the other writer stored.
export async function applyOutcomes(endpointId, outcomes) {
  const r = await redis();
  for (let i = 0; i < MAX_CAS_RETRIES; i++) {
    const raw = await r.get(key(endpointId));
    const before = parse(raw);
    const after = outcomes.reduce((b, o) => cb.record(b, o.ok, o.at), before);
    const next = JSON.stringify(after);
    if (next === (raw ?? '')) return after;
    const written = await r.eval(CAS_SCRIPT, { keys: [key(endpointId)], arguments: [raw ?? '', next, String(TTL_SECONDS)] });
    if (written === 1) {
      const from = cb.current(before, outcomes[0]?.at ?? Date.now()).state;
      if (after.state !== from) console.log(`Circuit breaker ${endpointId}: ${from} -> ${after.state}`);
      return after;
    }
  }
  throw new Error(`circuit breaker ${endpointId}: gave up after ${MAX_CAS_RETRIES} conflicting writes`);
}

export const recordOutcome = (endpointId, ok) => applyOutcomes(endpointId, [{ ok, at: Date.now() }]);

// Batch read for the consumer: one MGET for many endpoints.
export async function getBreakers(endpointIds) {
  if (!endpointIds.length) return new Map();
  const raws = await (await redis()).mGet(endpointIds.map(key));
  const now = Date.now();
  return new Map(endpointIds.map((id, i) => [id, cb.current(parse(raws[i]), now)]));
}

// When an open breaker will next allow a trial delivery.
export const reopensAt = (b) => new Date(b.openedAt + cb.DEFAULT_CONFIG.cooldownMs);
