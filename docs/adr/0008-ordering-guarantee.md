# ADR-008: Ordering guarantee, with a probe to separate a bad endpoint from a bad event

**Status:** Accepted (Day 4). **Differs from the spec**, which chose plain initiation-order.

## Context

The spec (v2 ADR-008) commits to **initiation-order**: delivery attempts *start* in acceptance order, but outcomes may finish in any order. It rejects **completion-order** (event N+1 waits until N reaches a final state) because one retrying event would block its scope for the full retry schedule.

The code, built on Day 2, does **completion-order per endpoint**:

- `consumer.js`: if the endpoint has any `pending` row, a new event is queued behind it without an inline attempt.
- `delivery-worker.js`: `DISTINCT ON (endpoint_id) ... ORDER BY created_at` only offers each endpoint's oldest pending row.

With the default schedule (1m, 5m, 30m, 2h, 12h), an event that keeps failing held the head of its endpoint's queue for about **14.6 hours**. That's harmless when the whole endpoint is down, because nothing would get through anyway. It's harmful when the endpoint is fine and rejects **just this one event**: a malformed payload, or a handler bug for one specific record. Then one bad event stalls all of that endpoint's traffic.

The key observation: when an event keeps failing, the system has no idea whether **the endpoint** is broken or **the event** is. The right response is opposite in the two cases.

## Decision

Keep completion-order per endpoint, and **ask the endpoint which case it is**.

From the 3rd failed attempt on (`PROBE_AFTER` in `delivery-worker.js`: the inline attempt plus 2 retries, about 6 minutes), every further failure is followed by a **ping**: a signed `reliant.ping` event with a fresh id and an empty payload (`pingEndpoint` in `lib/deliver.js`).

| Ping result | Meaning | Action |
|---|---|---|
| 2xx | Endpoint works, but refuses this event | Dead-letter it as **`payload_rejected`**; the next event moves to the front |
| fails | Endpoint is down | Keep this event at the front, continue the backoff; eventually `delivery_exhausted` |

The ping result also feeds the circuit breaker. A healthy ping resets the failure count, so one bad event can't open the breaker for a healthy endpoint.

**The guarantee, as customers should read it:** events to an endpoint are delivered in the order they were accepted. An exception is made only when the endpoint has been shown to be healthy and still refuses an event 3 times; that event is set aside, never dropped. It sits in the DLQ with reason `payload_rejected` and can be replayed from the dashboard once the receiver is fixed.

**Receiver contract:** answer `reliant.ping` with 2xx and do nothing else. It's signed like every delivery, so receivers can verify it.

## Consequences

- Worst-case head-of-line stall for an event the endpoint keeps rejecting drops from about 14.6 hours to about 6 minutes, i.e. 3 attempts. A genuinely down endpoint still gets the full retry schedule, as before.
- An endpoint that is down receives up to 4 extra ping requests per event (after attempts 3 to 6). That's negligible, and the circuit breaker stops both pings and attempts once it opens.
- **A set-aside event breaks order once:** later events are delivered before it, and a replay delivers it out of order. Receivers that care must reconcile with event timestamps, which they should do anyway under at-least-once delivery.
- **Misclassification is possible.** A receiver that answers pings without checking anything (a static 200) but fails *every* real event because its handler is broken will have each event set aside after 3 attempts instead of retried for 14 hours. The events aren't lost; they all land in the DLQ and can be replayed in bulk after the fix. The reverse also happens: an endpoint that fails pings for reasons unrelated to its health gets the full backoff, which is the old behaviour.
- The ping isn't a ledger row, because it isn't an event (the ledger has a foreign key to `events`). The decision is recorded on the dead-lettered row instead: `dead_reason` plus `last_error` ("endpoint answered a ping fine, so this event is the problem").

## Alternatives considered

- **Initiation-order (the spec):** no stalls at all, but receivers lose arrival order for every event, not just set-aside ones.
- **Treat permanent 4xx responses (400, 404, 410, 422) as immediately fatal:** cheaper (no extra request), but it trusts the status code, which many receivers get wrong. A 500 caused by a bad payload is common, and a 4xx can be temporary (a 404 during a deploy). The ping tests the endpoint directly instead of guessing from the status code.
- **Use the next queued real event as the probe instead of a synthetic ping:** needs no receiver contract, but delivers an event out of order *before* knowing whether the skip is justified, and there's nothing to probe with when the queue has only one event.
- **A per-endpoint `ordering: strict | relaxed` setting:** possible later if customers ask for it; not justified yet.
