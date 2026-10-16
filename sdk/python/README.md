# reliant-webhooks (Python)

Receive [Reliant](../../README.md) webhooks: verify the signature, reject replays, skip health pings, and handle each event once even though Reliant delivers at least once. Standard library only, Python 3.9+.

```sh
pip install ./sdk/python        # from a checkout of the repo
```

## Use

```python
from reliant_webhooks import Receiver, Webhook, WebhookVerificationError

def handle(event):              # event.id, event.type, event.tenant_id, event.payload
    save_order(event.payload)   # raise to make Reliant retry later

receiver = Receiver(Webhook(["whsec_new...", "whsec_old..."]), handle)

# In your web framework's POST handler, with the RAW body (not re-serialized JSON):
try:
    receiver.handle(raw_body, request_headers)  # Outcome.HANDLED / DUPLICATE / PING
    return 200
except WebhookVerificationError:
    return 401
except Exception:
    return 500                  # Reliant retries any non-2xx
```

`examples/receiver.py` is a complete receiver on `http.server`.

## What it checks

| Check | Why |
|---|---|
| HMAC-SHA256 over `id.timestamp.body`, constant-time compare | Only Reliant, holding your secret, can produce it. Changing the id, timestamp or body breaks it |
| Timestamp within 5 minutes, either direction | A captured request can't be replayed later |
| Any of your secrets against any signature in the header | During a secret rotation Reliant signs with both secrets for 24 hours, so you can roll out the new one at your own pace |
| `reliant.ping` events skip your handler | Reliant pings an endpoint to tell a broken endpoint from one bad event; answering 2xx is all it needs |
| Dedup on `webhook-id` | A retry or a redelivery after a consumer failover carries the same id |

## Dedup: what "once" means

`Receiver` claims the event id before calling your handler, marks it done when the handler returns, and releases it if the handler raises, so Reliant's retry can run it again. Two copies arriving at once run the handler once.

`InMemoryDeduplicator` (the default) works within one process: ids are kept 24 hours, a claim whose handler never finished expires after 5 minutes, and past 100,000 ids the oldest are forgotten. For more than one process, implement the three-method `Deduplicator` protocol on shared storage, e.g. an `INSERT` into a table with a unique key on the event id, done in the same database transaction as your handler's writes. That is the only way to get exactly-once handling across a crash between "handled" and "marked done".

## Standard Webhooks compatible

Reliant signs with the [Standard Webhooks](https://www.standardwebhooks.com/) scheme: the same headers, and the HMAC key is the base64-decoded part of the secret after `whsec_`. Its official libraries (any language) verify Reliant deliveries too. The tests check this against the published test vector and, when `standardwebhooks` is installed (CI pins 1.1.0), against the library itself in both directions. What this SDK adds on top is the dedup and ping handling.

## Tests

```sh
cd sdk/python && python -m unittest discover -s tests -v
```

The cross-language tests sign with Reliant's own `core/signing.js` (they need `node`), so they fail if the SDK and the server ever disagree. `pip install standardwebhooks` enables the official-library test.
