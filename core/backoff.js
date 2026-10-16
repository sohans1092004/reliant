// Pure retry policy. No I/O - the same function drives both retry-publisher.js
// (Kafka publish retries) and delivery-worker.js (HTTP delivery retries).

// Spec default: 1m, 5m, 30m, 2h, 12h. One initial attempt + one retry per entry,
// so 6 attempts in total before the event is dead-lettered.
export const DEFAULT_SCHEDULE_MINUTES = [1, 5, 30, 120, 720];

// attemptsMade = how many attempts have already failed (>= 1).
// Returns the delay in ms before the next attempt, or null if retries are exhausted.
// Jitter is +/-10% so a batch that failed together doesn't retry in lockstep.
export function nextRetryDelay(attemptsMade, {
  schedule = DEFAULT_SCHEDULE_MINUTES,
  unitMs = 60_000,
  jitter = 0.1,
  random = Math.random,
} = {}) {
  if (!Number.isInteger(attemptsMade) || attemptsMade < 1) throw new Error(`bad attemptsMade: ${attemptsMade}`);
  if (attemptsMade > schedule.length) return null;
  const base = schedule[attemptsMade - 1] * unitMs;
  return Math.round(base * (1 - jitter + 2 * jitter * random()));
}

// A tenant-supplied schedule. Bounded so a tenant can't make Reliant retry forever or
// hammer an endpoint: 1-10 retries, each 1 minute to 24 hours apart.
export function validateRetrySchedule(schedule) {
  if (!Array.isArray(schedule) || schedule.length < 1 || schedule.length > 10) return 'must be a list of 1-10 delays in minutes';
  if (!schedule.every((m) => Number.isInteger(m) && m >= 1 && m <= 1440)) return 'each delay must be a whole number of minutes from 1 to 1440';
  return null;
}

// A 429 or 503's Retry-After (delay in seconds, or an HTTP date) in ms, or null when it's
// missing or unreadable. Capped at a day, the longest delay a tenant's own schedule allows.
export const MAX_RETRY_AFTER_MS = 24 * 3600_000;
export function parseRetryAfter(value, now = Date.now()) {
  if (value == null) return null;
  const v = String(value).trim(); // an HTTP date ends in GMT; unchecked, Date.parse('-5') is a year
  const ms = /^\d+$/.test(v) ? Number(v) * 1000 : /GMT$/.test(v) ? Date.parse(v) - now : NaN;
  if (!Number.isFinite(ms)) return null;
  return Math.min(Math.max(ms, 0), MAX_RETRY_AFTER_MS);
}
