# Incident 004: a frozen consumer came back as a zombie and re-delivered events

**Date:** 2026-10-16 · **Type:** induced failure (spec §9 #6) · **Drill:** `LT_SINK_HOST=host.docker.internal npm run loadtest -- consumer-freeze` · **Status:** resolved

## Summary

Spec failure scenario #6: a consumer is cut off from Kafka long enough for the group to hand its partitions to another consumer, then comes back still holding a half-processed batch, as a "zombie". The spec asks that the zombie's leftover work not race the new owner's.

The drill runs two consumers and a worker as containers. At 50 events/s, it freezes one consumer with `docker pause` for 45s (longer than the 30s session timeout), then resumes it.

**Nothing was lost and order held, but the customer got about 137 duplicate deliveries per run.** Almost all of them came from the zombie in the first 3 seconds after it resumed, for events the new owner had already delivered.

## Impact (two runs before the fix)

| | Run 1 | Run 2 |
|---|---|---|
| Accepted / delivered | 3,033 / 3,033 | 3,066 / 3,066 |
| Duplicate deliveries | 138 | 137: **136 within 5s of the resume**, 1 during the freeze |
| Per-endpoint order inversions | 0 | 0 |
| Ledger | exact | exact |

## Root cause

The consumer checked group membership with kafkajs's `heartbeat()` *after* each inline delivery attempt. `heartbeat()` only contacts the broker when 3 seconds have passed since the last one. So a resumed zombie spent up to 3 seconds working through the batch it already held, POSTing to customers, before a heartbeat told it the group had moved on. The ledger stayed exact only because the chunk then threw before its write step.

A second gap showed up in the fix's first verification run. If the zombie's only remaining attempt was the request in flight at the moment of the freeze, it timed out during the freeze, and no further attempt in that chunk meant no further membership check. The chunk then **finished normally**, wrote a retry-queue row, and a worker delivered the event a third time. The ledger showed 3,067 rows for 3,066 events.

## Fix (`consumer.js`)

- **Check membership before every inline attempt and before the chunk's writes** (`stillMember`). If 3s have passed, which is always true after a freeze longer than that, `heartbeat()` contacts the broker and fails for a member the group no longer knows. The chunk then throws: nothing more is delivered, written or committed.
- **The check is process-wide.** One partition's batch learning that membership is lost stops the batches of all other partitions too. It's cleared on `GROUP_JOIN`.
- **Nothing is lost by stopping:** the new owner reprocesses those messages from the last committed offset.

## Verification (after the fix)

| | Run 3 | Run 4 | Run 5 |
|---|---|---|---|
| Accepted / delivered | 3,060 / 3,060 | 3,046 / 3,046 | 3,023 / 3,023 |
| Duplicate deliveries | 0 | 0 | 1 (during the freeze) |
| Ledger rows / events | exact | exact | exact |

Run 5 hit the edge case again (a request in flight at the freeze). This time the zombie wrote nothing and no worker re-delivered. The one remaining duplicate is that in-flight request itself: it reached the receiver just before the freeze, the zombie never recorded it, and the new owner re-delivered it, which is correct. That one is inherent to at-least-once delivery: a consumer can't know whether a request it was cut off in the middle of arrived. Receivers dedupe on `webhook-id`.

## Notes

- **Why Docker and not Kubernetes:** freezing one pod's process needs a privileged node-debug pod. That was declined as too much access for a drill, so the consumers ran as containers that join Redpanda's network namespace and were frozen with `docker pause`.
- **Remaining window:** a consumer that is merely *slow* (not frozen) for under the session timeout keeps its partitions, and a normal heartbeat keeps it in the group. That's correct, not a zombie.
