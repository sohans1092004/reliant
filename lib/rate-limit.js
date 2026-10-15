// Token bucket in Redis, one bucket per key (tenant). The whole check-refill-take runs in
// one Lua script, so it's atomic across any number of ingest processes, and it uses
// Redis's clock (TIME), so processes with skewed clocks still share one bucket.
//
// Replaces a fixed 1-second window, which let a tenant send up to 2x its limit across a
// window edge. A bucket allows short bursts up to `burst`, then exactly `rate`/s.
import { redis } from './infra.js';

const BUCKET_SCRIPT = `
local rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens, ts = tonumber(b[1]) or burst, tonumber(b[2]) or now
tokens = math.min(burst, tokens + (now - ts) * rate / 1000)
local allowed = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return allowed`;

// true = request may proceed. Fails open if Redis is down: an outage of the limiter
// shouldn't become an outage of ingest.
export async function takeToken(key, ratePerSec, burst) {
  try {
    const r = await redis();
    return (await r.eval(BUCKET_SCRIPT, { keys: [`rl:${key}`], arguments: [String(ratePerSec), String(burst)] })) === 1;
  } catch {
    return true;
  }
}
