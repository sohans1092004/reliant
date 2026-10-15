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
