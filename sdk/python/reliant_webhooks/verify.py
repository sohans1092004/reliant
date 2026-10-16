"""Signature verification for Reliant webhook deliveries.

Every delivery carries three headers:
    webhook-id:        the event id (stable across retries: dedupe on it)
    webhook-timestamp: unix seconds when this attempt was signed
    webhook-signature: space-separated "v1,<base64 HMAC-SHA256>" entries, one per
                       secret Reliant signs with (two during a secret rotation)

The signed content is "<id>.<timestamp>.<raw body>", and the HMAC key is the
base64-decoded part of the secret after "whsec_". That is the Standard Webhooks scheme,
so its official libraries verify Reliant deliveries too; this SDK adds dedup and ping
handling on top.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Union

PING_TYPE = "reliant.ping"
DEFAULT_TOLERANCE_SECONDS = 300  # same window Reliant's own verify() uses


class WebhookVerificationError(Exception):
    """The request is not a valid, fresh Reliant delivery. Answer it with 401."""


@dataclass(frozen=True)
class Event:
    id: str
    type: str
    tenant_id: str
    payload: Any
    created_at: str
    timestamp: int  # when this attempt was signed, not when the event happened

    @property
    def is_ping(self) -> bool:
        """Reliant's health probe: answer 2xx and do nothing else."""
        return self.type == PING_TYPE


def signing_key(secret: str) -> bytes:
    """The HMAC key inside a "whsec_<base64>" secret. Strict, like Reliant's own check."""
    encoded = secret[len("whsec_"):] if secret.startswith("whsec_") else secret
    try:
        key = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        key = b""
    if not key:
        raise ValueError("signing secret must be whsec_<base64>")
    return key


def sign(secret: str, msg_id: str, timestamp: int, body: bytes) -> str:
    """One signature entry, exactly as Reliant computes it. Useful in tests."""
    content = f"{msg_id}.{timestamp}.".encode() + body
    digest = hmac.new(signing_key(secret), content, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode()


class Webhook:
    """Verifies deliveries for one endpoint.

    Pass every secret you currently accept. During a rotation, keep the old secret
    alongside the new one until you've deployed the new one everywhere; Reliant signs
    with both for 24 hours, so either is enough to verify.
    """

    def __init__(self, secrets: Union[str, Iterable[str]], *, tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
                 clock: Callable[[], float] = time.time):
        self._secrets = [secrets] if isinstance(secrets, str) else list(secrets)
        if not self._secrets or not all(isinstance(s, str) and s for s in self._secrets):
            raise ValueError("at least one non-empty secret is required")
        for s in self._secrets:
            signing_key(s)  # fail at startup on a mistyped secret, not on every request
        self._tolerance = tolerance_seconds
        self._clock = clock

    def verify(self, body: Union[bytes, str], headers: Mapping[str, str]) -> Event:
        """Checks the signature and timestamp, then parses the body.

        `body` must be the raw request body, before any JSON parsing: re-serialized
        JSON is not byte-identical and won't verify.
        """
        raw = body.encode("utf-8") if isinstance(body, str) else bytes(body)
        h = {k.lower(): v for k, v in headers.items()}
        msg_id, ts_header, sig_header = h.get("webhook-id"), h.get("webhook-timestamp"), h.get("webhook-signature")
        if not msg_id or not ts_header or not sig_header:
            raise WebhookVerificationError("missing webhook-id, webhook-timestamp or webhook-signature header")

        try:
            timestamp = int(ts_header)
        except ValueError:
            raise WebhookVerificationError("webhook-timestamp is not an integer") from None
        # Both directions: an old timestamp is a replay, a far-future one could be replayed later.
        if abs(self._clock() - timestamp) > self._tolerance:
            raise WebhookVerificationError("timestamp outside the tolerance window (replayed or clock skew)")

        given = sig_header.split(" ")
        expected = (sign(secret, msg_id, timestamp, raw) for secret in self._secrets)
        # compare_digest: constant time, so the comparison doesn't leak how much matched.
        if not any(hmac.compare_digest(e, g) for e in expected for g in given):
            raise WebhookVerificationError("no signature matches")

        try:
            data = json.loads(raw)
            return Event(id=data["event_id"], type=data["type"], tenant_id=data["tenant_id"],
                         payload=data.get("payload"), created_at=data.get("created_at"), timestamp=timestamp)
        except (ValueError, KeyError, TypeError) as err:
            # Signed by Reliant but not a shape we know: a bug on one side, not an attack.
            raise WebhookVerificationError(f"signed body is not a Reliant event: {err}") from err
