"""Run from sdk/python:  python -m unittest -v

The cross-language tests sign with Reliant's own Node code (core/signing.js), so they
fail if the SDK and the server ever disagree. They're skipped when node isn't installed.
"""
import json
import shutil
import subprocess
import threading
import time
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from reliant_webhooks import (Event, InMemoryDeduplicator, Outcome, Receiver, Webhook,
                              WebhookVerificationError, sign)

try:
    from standardwebhooks import webhooks as standardwebhooks
except ImportError:
    standardwebhooks = None

REPO = Path(__file__).resolve().parents[3]
SECRET, OLD_SECRET = "whsec_" + "a" * 43 + "=", "whsec_" + "b" * 43 + "="
NODE = shutil.which("node")


def reliant_signature(secrets, msg_id, timestamp, body: str) -> str:
    """webhook-signature exactly as Reliant's delivery code builds it."""
    script = ("import { signatureHeader } from %s;"
              "const [s, id, ts, body] = JSON.parse(process.argv[1]);"
              "process.stdout.write(signatureHeader(s, id, ts, body));") % json.dumps((REPO / "core" / "signing.js").as_uri())
    return subprocess.run([NODE, "--input-type=module", "-e", script, json.dumps([secrets, msg_id, timestamp, body])],
                          check=True, capture_output=True, text=True).stdout


def make_body(msg_id, event_type="order.created", payload=None) -> str:
    """The JSON body, shaped like lib/deliver.js sends it."""
    return json.dumps({"event_id": msg_id, "tenant_id": "tenant_a", "type": event_type,
                       "payload": {"n": 1} if payload is None else payload, "created_at": "2026-10-16T00:00:00.000Z"},
                      separators=(",", ":"), ensure_ascii=False)


def headers_for(msg_id, timestamp, signature):
    return {"Webhook-Id": msg_id, "Webhook-Timestamp": str(timestamp), "Webhook-Signature": signature}


def delivery(event_type="order.created", payload=None, secrets=(SECRET,), timestamp=None, msg_id=None):
    """(body bytes, headers) for one delivery, signed by the SDK's sign()."""
    msg_id = msg_id or str(uuid.uuid4())
    timestamp = int(time.time()) if timestamp is None else timestamp
    body = make_body(msg_id, event_type, payload).encode()
    return body, headers_for(msg_id, timestamp, " ".join(sign(s, msg_id, timestamp, body) for s in secrets))


@unittest.skipUnless(NODE, "node not installed")
class MatchesReliantServer(unittest.TestCase):
    def verify_signed_by_reliant(self, secrets, receiver_secret, payload=None):
        msg_id, ts = str(uuid.uuid4()), int(time.time())
        text = make_body(msg_id, payload=payload)
        headers = headers_for(msg_id, ts, reliant_signature(list(secrets), msg_id, ts, text))
        return Webhook(receiver_secret).verify(text.encode(), headers), headers

    def test_signature_from_reliant_verifies(self):
        event, headers = self.verify_signed_by_reliant([SECRET], SECRET)
        self.assertEqual(event.id, headers["Webhook-Id"])

    def test_non_ascii_body(self):
        payload = {"name": "Zoë ☕ 東京"}
        self.assertEqual(self.verify_signed_by_reliant([SECRET], SECRET, payload)[0].payload, payload)

    def test_rotation_header_verifies_with_either_secret(self):
        _, headers = self.verify_signed_by_reliant([SECRET, OLD_SECRET], OLD_SECRET)  # receiver not yet switched
        self.assertEqual(len(headers["Webhook-Signature"].split(" ")), 2)
        self.verify_signed_by_reliant([SECRET, OLD_SECRET], SECRET)                   # receiver already switched

    def test_reliant_signature_with_the_wrong_secret_fails(self):
        with self.assertRaises(WebhookVerificationError):
            self.verify_signed_by_reliant([SECRET], OLD_SECRET)


class Verification(unittest.TestCase):
    def test_valid(self):
        body, headers = delivery()
        event = Webhook(SECRET).verify(body, headers)
        self.assertEqual((event.type, event.tenant_id, event.payload), ("order.created", "tenant_a", {"n": 1}))

    def test_tampered_body(self):
        body, headers = delivery()
        with self.assertRaises(WebhookVerificationError):
            Webhook(SECRET).verify(body.replace(b'"n":1', b'"n":2'), headers)

    def test_tampered_id_or_timestamp(self):
        body, headers = delivery()
        for key, value in (("Webhook-Id", str(uuid.uuid4())), ("Webhook-Timestamp", str(int(headers["Webhook-Timestamp"]) + 1))):
            with self.subTest(key), self.assertRaises(WebhookVerificationError):
                Webhook(SECRET).verify(body, {**headers, key: value})

    def test_wrong_secret(self):
        with self.assertRaises(WebhookVerificationError):
            Webhook(OLD_SECRET).verify(*delivery())

    def test_stale_and_future_timestamps(self):
        now = time.time()
        for offset in (-301, 301):
            with self.subTest(offset), self.assertRaisesRegex(WebhookVerificationError, "tolerance"):
                Webhook(SECRET, clock=lambda: now).verify(*delivery(timestamp=int(now) + offset))
        Webhook(SECRET, clock=lambda: now).verify(*delivery(timestamp=int(now) - 299))

    def test_missing_or_malformed_headers(self):
        body, headers = delivery()
        for broken in ({k: v for k, v in headers.items() if k != "Webhook-Signature"},
                       {**headers, "Webhook-Timestamp": "soon"},
                       {**headers, "Webhook-Signature": "v1,not-base64 garbage"}):
            with self.assertRaises(WebhookVerificationError):
                Webhook(SECRET).verify(body, broken)

    def test_one_good_signature_among_unknown_ones_is_enough(self):
        body, headers = delivery()
        headers["Webhook-Signature"] = "v2,future-scheme " + headers["Webhook-Signature"]
        Webhook(SECRET).verify(body, headers)

    def test_signed_but_not_an_event(self):
        msg_id, ts = str(uuid.uuid4()), int(time.time())
        body = b'{"hello":"world"}'
        headers = {"webhook-id": msg_id, "webhook-timestamp": str(ts), "webhook-signature": sign(SECRET, msg_id, ts, body)}
        with self.assertRaisesRegex(WebhookVerificationError, "not a Reliant event"):
            Webhook(SECRET).verify(body, headers)

    def test_needs_a_secret(self):
        for bad in ("", [], [""]):
            with self.subTest(bad), self.assertRaises(ValueError):
                Webhook(bad)


class StandardWebhooks(unittest.TestCase):
    """Reliant uses the Standard Webhooks signing scheme, so either side can be swapped."""

    def test_published_test_vector(self):
        body = b'{"test": 2432232314}'
        self.assertEqual(sign("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", "msg_p5jXN8AQM9LWM0D4loKWxJek", 1614265330, body),
                         "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=")

    @unittest.skipUnless(standardwebhooks, "pip install standardwebhooks to run")
    def test_official_library_verifies_reliant_and_vice_versa(self):
        msg_id, ts = str(uuid.uuid4()), int(time.time())
        text = make_body(msg_id)
        if NODE:  # signed by Reliant's server code itself
            standardwebhooks.Webhook(SECRET).verify(text, headers_for(msg_id, ts, reliant_signature([SECRET], msg_id, ts, text)))
        official = standardwebhooks.Webhook(SECRET).sign(msg_id, datetime.fromtimestamp(ts, tz=timezone.utc), text)
        self.assertEqual(Webhook(SECRET).verify(text.encode(), headers_for(msg_id, ts, official)).id, msg_id)

    def test_malformed_secret_is_refused_up_front(self):
        for bad in ("whsec_", "whsec_not base64!", "whsec_abc"):
            with self.subTest(bad), self.assertRaisesRegex(ValueError, "whsec_<base64>"):
                Webhook(bad)


class Handling(unittest.TestCase):
    def setUp(self):
        self.handled = []
        self.receiver = Receiver(Webhook(SECRET), self.handled.append)

    def test_redelivery_is_handled_once(self):
        body, headers = delivery()
        self.assertEqual(self.receiver.handle(body, headers), Outcome.HANDLED)
        # A retry re-signs with a new timestamp; the event id stays the same.
        msg_id = headers["Webhook-Id"]
        body2, headers2 = delivery(msg_id=msg_id, timestamp=int(time.time()) + 5)
        self.assertEqual(self.receiver.handle(body2, headers2), Outcome.DUPLICATE)
        self.assertEqual([e.id for e in self.handled], [msg_id])

    def test_ping_skips_the_handler(self):
        self.assertEqual(self.receiver.handle(*delivery(event_type="reliant.ping", payload={})), Outcome.PING)
        self.assertEqual(self.handled, [])

    def test_failed_handler_lets_the_retry_run(self):
        calls = []

        def flaky(event: Event):
            calls.append(event.id)
            if len(calls) == 1:
                raise RuntimeError("database down")

        receiver = Receiver(Webhook(SECRET), flaky)
        body, headers = delivery()
        with self.assertRaises(RuntimeError):
            receiver.handle(body, headers)
        self.assertEqual(receiver.handle(body, headers), Outcome.HANDLED)
        self.assertEqual(len(calls), 2)

    def test_bad_signature_never_reaches_dedup_or_handler(self):
        body, headers = delivery()
        with self.assertRaises(WebhookVerificationError):
            self.receiver.handle(body + b" ", headers)
        self.assertEqual(self.receiver.handle(body, headers), Outcome.HANDLED)

    def test_concurrent_duplicates_run_the_handler_once(self):
        body, headers = delivery()
        started, gate = threading.Event(), threading.Event()

        def slow(event):
            started.set()
            gate.wait(5)
            self.handled.append(event)

        receiver = Receiver(Webhook(SECRET), slow)
        results = []
        first = threading.Thread(target=lambda: results.append(receiver.handle(body, headers)))
        first.start()
        started.wait(5)
        results.append(receiver.handle(body, headers))  # arrives while the first is still running
        gate.set()
        first.join(5)
        self.assertEqual(sorted(r.value for r in results), ["duplicate", "handled"])
        self.assertEqual(len(self.handled), 1)


class DedupStore(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0
        self.store = InMemoryDeduplicator(ttl_seconds=60, processing_timeout_seconds=10, max_entries=3,
                                          clock=lambda: self.now)

    def test_done_ids_expire_after_the_ttl(self):
        self.assertTrue(self.store.claim("a"))
        self.store.complete("a")
        self.now += 59
        self.assertFalse(self.store.claim("a"))
        self.now += 2
        self.assertTrue(self.store.claim("a"))

    def test_an_abandoned_claim_expires(self):
        self.assertTrue(self.store.claim("a"))  # handler hung or the process died
        self.assertFalse(self.store.claim("a"))
        self.now += 11
        self.assertTrue(self.store.claim("a"))

    def test_oldest_ids_are_forgotten_past_the_cap(self):
        for i in "abcd":
            self.store.claim(i)
            self.store.complete(i)
        self.assertEqual(len(self.store), 3)
        self.assertTrue(self.store.claim("a"))   # forgotten
        self.assertFalse(self.store.claim("d"))  # still remembered


if __name__ == "__main__":
    unittest.main()
