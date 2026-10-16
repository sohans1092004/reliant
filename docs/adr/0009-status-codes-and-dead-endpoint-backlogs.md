# ADR-009: Status codes, and the backlog of an endpoint that stays down

**Status:** Accepted. Not in the spec, which says only that a non-2xx is retried.

## Context

Until now, every non-2xx answer was handled the same way: retry on the tenant's schedule, ping after 3 failures (ADR-008), dead-letter after the last retry. A review found two problems with that.

**The receiver's own instructions were ignored.**
- A `429 Too Many Requests` or `503` with `Retry-After: 120` was retried on the schedule instead. On the first retry that's 1 minute, so the receiver was hit before it said it was ready.
- A `410 Gone` ("this URL is gone for good") was retried for 14.6 hours, then the next event repeated the cycle, forever.

**A long-dead endpoint's backlog cleared at one event per retry cycle.** Ordering (ADR-008) keeps every later event queued behind the endpoint's oldest pending one, and only that head is ever attempted. When the head finally exhausted its retries, the next event became the head and started its *own* full cycle. K queued events took about K × 14.6 hours to reach the DLQ: 100 events behind a dead endpoint sat for two months, invisible to the DLQ alerts.

## Decision

**429 / 503 with `Retry-After`:** the next attempt waits as long as the receiver asked (seconds or an HTTP date, capped at 24 hours, the longest delay a tenant's schedule allows). The attempt still counts, so an endpoint that throttles forever still ends in the DLQ. A 429 doesn't trigger the ADR-008 ping: the endpoint is throttling, and a ping it happens to let through would wrongly blame the event. Without `Retry-After`, both are ordinary failures.

**410 Gone:** Reliant disables the endpoint (`is_active = false`, `disabled_reason = 'http_410'`) and dead-letters everything queued for it as `endpoint_gone`. The dashboard shows why the endpoint is off. An owner turns it back on (which clears the reason) and replays the dead letters. A 410 at the inline attempt is handed to the worker at once (one more request) so there's one code path that disables.

**Exhausted head on a failing endpoint:** when the head runs out of retries and the endpoint fails the ping too, every other pending row of that endpoint is dead-lettered with it, as `endpoint_unreachable`, in the same statement. So no event waits longer than one retry cycle. `POST /api/endpoints/:id/replay-dead` re-queues them all in their original order once the endpoint is back.

A healthy ping still sets aside only the one event (`payload_rejected`), as before: then the endpoint works and the events behind it should go through.

## Consequences

- Worst-case time to the DLQ for any event behind a dead endpoint: about 14.6 hours with the default schedule, instead of K × 14.6 hours.
- **Events behind the head get no attempt of their own.** An event queued one minute before the head gives up goes to the DLQ after one minute with `attempt_count = 0`. That's deliberate: the endpoint has failed every request for the whole cycle, and Reliant can't deliver that event before the head anyway. `last_error` names the head it was queued behind.
- **New events keep queueing** after the backlog is cleared, unless the endpoint is disabled. The next event starts a fresh cycle, so a permanently dead endpoint produces one batch of dead letters per ~14.6 hours. Auto-disabling after repeated cycles is a possible next step; it isn't built.
- **One 410 disables an endpoint.** A receiver that answers 410 by mistake loses deliveries until an owner re-enables it and replays. 410 is the one code trusted that far because it means exactly this, and it's what Standard Webhooks and the large providers use as the "unsubscribe" signal. Other 4xx codes still aren't trusted (ADR-008: a 404 during a deploy is common).
- New metrics: `reliant_endpoints_disabled_total{reason}`, and `reliant_dead_letters_total` gains the `endpoint_unreachable` and `endpoint_gone` reasons.

## Alternatives considered

- **Give each queued event its own attempts in parallel with the head:** fast, but breaks per-endpoint order (ADR-008) for every event, not just failed ones.
- **Keep the backlog pending and only alert:** nothing is dead-lettered early, but the events still wait K cycles, and the DLQ (the place customers look) doesn't show them.
- **Auto-disable on any exhausted cycle:** stops the repeating cycles, but one bad night would turn off a customer's endpoint. Too aggressive without a notification channel to tell them.
- **Honour `Retry-After` without counting the attempt:** kinder to receivers, but a receiver that always returns 429 would never reach the DLQ.

## How it's tested

`tests/pipeline.test.js`, against real processes and a receiver that answers with these codes:
- 410: both queued events are dead-lettered as `endpoint_gone`, the second was never sent, the endpoint is off with `disabled_reason = http_410`, a new event isn't sent, and turning it on clears the reason.
- 429 with `Retry-After: 2`: both retries wait at least 1.9s (the test schedule is 1s), inline and in the worker; no ping is sent.
- Backlog: 4 events behind a failing endpoint all reach the DLQ together; only the head was ever sent; replay delivers all 4 in order.

Each test fails when its part of the worker is removed.
