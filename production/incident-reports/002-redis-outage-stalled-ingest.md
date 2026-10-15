# Incident 002: a Redis outage stalled ingest instead of failing open

**Date:** 2026-10-15 · **Type:** induced failure · **Drill:** `npm run loadtest -- redis-outage` · **Status:** resolved, regression test added

## Summary

Redis only holds disposable state: circuit-breaker state and rate-limit buckets. The design says Reliant **fails open** without it, meaning no rate limiting and every breaker treated as closed, but delivery carries on. We killed Redis for 30 seconds at 50 events/s to check.

It didn't fail open. **Every ingest request hung for the whole outage**, and so did the consumer. No accepted event was lost, but the slow recovery afterwards exposed a second problem: one endpoint's backlog drained at one event per 10 seconds.

## Impact (first run)

| | |
|---|---|
| Ingest responses during the outage | **none**. In-flight requests climbed from 161 to 1,122, and all completed only after Redis returned |
| Consumer | stalled, missed Kafka heartbeats, was removed from the group and re-joined |
| Accepted events | 2,687, all eventually delivered (none lost) |
| Recovery | 73 events queued behind one endpoint drained at **1 per 10s** (about 12 minutes) |

## Root causes

1. **node-redis queues commands while disconnected** (its "offline queue", on by default) and runs them when it reconnects. So `takeToken` never threw, its `catch → return true` fail-open path never ran, and every caller just waited. The same happened to the consumer's breaker reads.
2. **The worker slept the full poll interval even when it had just found work.** A sweep claims one row per endpoint (ADR-008 ordering), so a backlogged endpoint got one delivery per 10s `WORKER_POLL_MS`. The backlog itself came from the stall: one delivery was queued while Redis hung the consumer, and the ordering rule then queued every later event for that endpoint behind it.
3. **Found while writing the regression test:** node-redis's first `connect()` never settles while Redis is unreachable; it just keeps retrying. So a process that *started* during a Redis outage hung every Redis caller indefinitely. The drill didn't cover this, because Redis was always up when processes started.

## Fixes

| Fix | Where |
|---|---|
| `disableOfflineQueue: true`: commands reject at once while disconnected, so every caller's existing fail-open path runs | `lib/infra.js` |
| Callers wait for the first connect *or the first error*, never for `connect()` itself | `lib/infra.js` |
| The worker keeps sweeping, with no sleep, while a sweep claims rows; it waits `WORKER_POLL_MS` only when idle | `delivery-worker.js` |
| Regression test: with Redis unreachable, `takeToken` returns `true` in under 2s (it hung for 20s+ before) | `tests/resilience.test.js` |

## Verification (same drill, after the fixes)

```
responses by status: { '202': 2857 }
accepted: 2857, delivered at receiver: 2857, not delivered: 0
duplicate deliveries: 0
process exits: none
```

Not a single ingest error and no stall. During the outage, rate limiting and breakers were simply off, as designed. A third run after the connect fix: 2,773 of 2,773 delivered.

## Trade-offs accepted

- **Without Redis there's no rate limiting and no circuit breaking.** A tenant can exceed its limit, and a failing endpoint gets every inline attempt again (each capped at 1s, ADR-007). The alternative is refusing traffic because of a cache outage, which is worse.
- **The breaker-update error is logged once per chunk** (about 1,300 lines in a 30s outage). Noisy but harmless. Rate-limit it if Redis outages become routine.
