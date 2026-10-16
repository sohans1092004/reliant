"""Exactly-once *handling* on top of Reliant's at-least-once *delivery*.

Reliant may deliver the same event more than once: a retry after a timeout whose
request did arrive, or a consumer that lost its partition mid-request. The event id
(webhook-id) stays the same across all of them, so the receiver dedupes on it.
"""
from __future__ import annotations

import enum
import threading
import time
from collections import OrderedDict
from typing import Callable, Mapping, Protocol, Union

from .verify import Event, Webhook


class Deduplicator(Protocol):
    """Where handled event ids live. In-memory below; back it with Redis or your
    database (a unique constraint on the event id) when you run more than one process."""

    def claim(self, event_id: str) -> bool:
        """True if the caller should handle this event now: not handled, not in progress."""

    def complete(self, event_id: str) -> None:
        """The handler succeeded: refuse this id from now on."""

    def release(self, event_id: str) -> None:
        """The handler failed: let Reliant's retry handle it again."""


class InMemoryDeduplicator:
    """Thread-safe, one process only.

    - Handled ids are remembered for `ttl_seconds`. Reliant's default retry schedule
      spans about 14.6 hours, so the 24h default outlives every retry.
    - A claim that is never completed or released (the handler hung, or the process
      died mid-request) expires after `processing_timeout_seconds`, so a later retry
      isn't refused forever.
    - Past `max_entries`, the oldest ids are forgotten first. A forgotten id can be
      handled twice, so size it for your traffic over the TTL.
    """

    _IN_PROGRESS, _DONE = "in_progress", "done"

    def __init__(self, *, ttl_seconds: float = 24 * 3600, processing_timeout_seconds: float = 300,
                 max_entries: int = 100_000, clock: Callable[[], float] = time.monotonic):
        self._ttl, self._processing_timeout, self._max = ttl_seconds, processing_timeout_seconds, max_entries
        self._clock = clock
        self._entries: OrderedDict[str, tuple[str, float]] = OrderedDict()  # id -> (state, expires_at)
        self._lock = threading.Lock()

    def claim(self, event_id: str) -> bool:
        with self._lock:
            now = self._clock()
            entry = self._entries.get(event_id)
            if entry and entry[1] > now:
                return False
            self._entries[event_id] = (self._IN_PROGRESS, now + self._processing_timeout)
            self._entries.move_to_end(event_id)
            self._evict(now)
            return True

    def complete(self, event_id: str) -> None:
        with self._lock:
            self._entries[event_id] = (self._DONE, self._clock() + self._ttl)
            self._entries.move_to_end(event_id)

    def release(self, event_id: str) -> None:
        with self._lock:
            self._entries.pop(event_id, None)

    def __len__(self) -> int:
        return len(self._entries)

    def _evict(self, now: float) -> None:
        # Oldest first: drop expired entries at the front, then anything over the cap.
        while self._entries:
            oldest_id, (_, expires_at) = next(iter(self._entries.items()))
            if expires_at > now and len(self._entries) <= self._max:
                break
            del self._entries[oldest_id]


class Outcome(enum.Enum):
    HANDLED = "handled"      # your handler ran and returned
    DUPLICATE = "duplicate"  # already handled (or being handled): skipped
    PING = "ping"            # Reliant's health probe: skipped


class Receiver:
    """Verify, skip pings, dedupe, then call `handler(event)`.

    Map the result to HTTP like this:
      - returns any Outcome      -> 200 (a duplicate or ping is still a success)
      - WebhookVerificationError -> 401
      - your handler raised      -> 500, so Reliant retries it later
    Reliant treats every non-2xx as a failure and retries it, so only return 2xx once
    the event is safely handled.
    """

    def __init__(self, webhook: Webhook, handler: Callable[[Event], None], dedup: Deduplicator | None = None):
        self._webhook, self._handler = webhook, handler
        self._dedup = dedup if dedup is not None else InMemoryDeduplicator()

    def handle(self, body: Union[bytes, str], headers: Mapping[str, str]) -> Outcome:
        event = self._webhook.verify(body, headers)  # raises WebhookVerificationError
        if event.is_ping:
            return Outcome.PING
        if not self._dedup.claim(event.id):
            return Outcome.DUPLICATE
        try:
            self._handler(event)
        except BaseException:
            self._dedup.release(event.id)  # not handled: the retry must be able to run it
            raise
        self._dedup.complete(event.id)
        return Outcome.HANDLED
