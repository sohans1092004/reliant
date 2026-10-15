// Pure per-endpoint circuit breaker: closed -> open -> half_open -> closed.
// State is a plain object so it can be stored anywhere (Redis, in our case).
// No I/O and no clock reads - `now` is always passed in, which is what makes it testable.

export const DEFAULT_CONFIG = {
  failureThreshold: 5,     // consecutive failures (closed) before opening
  cooldownMs: 30_000,      // how long to stay open before allowing a trial
  halfOpenSuccesses: 2,    // consecutive trial successes needed to close again
};

export const initialState = () => ({ state: 'closed', failures: 0, successes: 0, openedAt: 0 });

// Effective state at time `now`: an open breaker whose cooldown has elapsed is half_open.
export function current(b, now, cfg = DEFAULT_CONFIG) {
  if (b.state === 'open' && now - b.openedAt >= cfg.cooldownMs) {
    return { ...b, state: 'half_open', successes: 0 };
  }
  return b;
}

// Returns the new state after one delivery outcome.
export function record(b, ok, now, cfg = DEFAULT_CONFIG) {
  b = current(b, now, cfg);
  const open = { state: 'open', failures: 0, successes: 0, openedAt: now };

  if (b.state === 'closed') {
    if (ok) return { ...b, failures: 0 };
    const failures = b.failures + 1;
    return failures >= cfg.failureThreshold ? open : { ...b, failures };
  }
  if (b.state === 'half_open') {
    if (!ok) return open;
    const successes = b.successes + 1;
    return successes >= cfg.halfOpenSuccesses ? initialState() : { ...b, successes };
  }
  // Still open (outcome from a request that started before it opened): a failure
  // restarts the cooldown, a success is ignored until the cooldown ends.
  return ok ? b : open;
}
