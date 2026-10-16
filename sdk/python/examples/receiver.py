"""A complete receiver on the standard library alone.

    RELIANT_WEBHOOK_SECRETS=whsec_new,whsec_old PORT=8080 python examples/receiver.py

Prints one line per request. Register http://<host>:8080/webhooks as the endpoint URL.
"""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # run from a checkout without installing
from reliant_webhooks import Receiver, Webhook, WebhookVerificationError  # noqa: E402

secrets = [s for s in os.environ.get("RELIANT_WEBHOOK_SECRETS", "").split(",") if s]
if not secrets:
    sys.exit("set RELIANT_WEBHOOK_SECRETS (comma-separated during a rotation)")


def handle_event(event):
    # Your business logic. Raise to make Reliant retry; return once it's safely done.
    print(json.dumps({"handled": event.id, "type": event.type, "payload": event.payload}), flush=True)


receiver = Receiver(Webhook(secrets), handle_event)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            outcome = receiver.handle(body, dict(self.headers))
            status = 200
            print(json.dumps({"outcome": outcome.value, "id": self.headers.get("webhook-id")}), flush=True)
        except WebhookVerificationError as err:
            status = 401
            print(json.dumps({"rejected": str(err)}), flush=True)
        except Exception as err:  # the handler failed: 500 makes Reliant retry later
            status = 500
            print(json.dumps({"handler_failed": repr(err)}), flush=True)
        self.send_response(status)
        self.end_headers()

    def log_message(self, *args):  # the JSON lines above are the log
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(json.dumps({"listening": port}), flush=True)
    ThreadingHTTPServer(("", port), Handler).serve_forever()
