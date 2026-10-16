# Running Reliant live on one VM

Everything in one `docker compose` project, public over HTTPS with Caddy and a Let's Encrypt certificate. It's built for a demo or a small production. For the AWS setup (EKS, RDS, MSK) see `infra/terraform/`.

**What you need:**
- A Linux VM with Docker and the Compose plugin: 2 vCPU and 4 GB of RAM is enough for a demo. Oracle Cloud's Always Free Ampere VM works and costs nothing; every image here is multi-arch.
- A domain or subdomain with an A record pointing at the VM.
- Ports 80 and 443 open to the internet. Caddy needs 80 for the certificate challenge and redirects it to HTTPS.

## Steps

```bash
# 1. Code
git clone https://github.com/sohans1092004/reliant.git && cd reliant/deploy

# 2. Secrets
cp .env.example .env
# set DOMAIN, then fill each secret with: openssl rand -base64 32
# (SECRET_ENCRYPTION_KEY must be exactly that: 32 random bytes, base64)

# 3. Start. migrate runs first; every service waits for it.
docker compose up -d --build --wait

# 4. A tenant and its first owner. Each prints its secret once.
docker compose run --rm --no-deps ingest node create-tenant.js acme "Acme Inc"
docker compose run --rm --no-deps ingest node create-user.js acme you@example.com owner
```

Open `https://$DOMAIN`, sign in, and change the password.

**5. The demo receiver (optional).** It's the Python SDK's example receiver: it verifies every signature, skips pings and deduplicates on `webhook-id`.
1. In the dashboard, add the endpoint `https://$DOMAIN/demo-receiver/webhooks` and copy the secret it shows.
2. Put the secret in `.env` as `RELIANT_WEBHOOK_SECRETS=whsec_...`.
3. Start the receiver:
   ```bash
   docker compose --profile demo up -d receiver
   ```
4. Send an event:
   ```bash
   curl https://$DOMAIN/events -H "Authorization: Bearer $INGEST_TOKEN" -H 'Content-Type: application/json' \
     -d '{"tenant_id":"acme","type":"order.created","payload":{"hello":"world"}}'
   docker compose logs receiver   # {"handled": "<event_id>", ...}
   ```

Deliveries to the demo receiver go out through the VM's public address and back in through Caddy, like any customer endpoint. If your provider doesn't route a VM's traffic back to its own public IP ("hairpin"), the delivery times out. Run the receiver on another machine instead.

## No VM? A Cloudflare quick tunnel

Any machine with Docker can be the host, with no domain, no open ports and no account. `compose.tunnel.yaml` removes Caddy's ports and adds a `cloudflared` container that gives the stack a public `https://<random>.trycloudflare.com` address.

```bash
cd deploy && cp .env.example .env    # fill the secrets as above, and set DOMAIN=:80 (the tunnel does TLS)
docker compose -f compose.yaml -f compose.tunnel.yaml up -d --build --wait
docker compose -f compose.yaml -f compose.tunnel.yaml logs tunnel | grep trycloudflare.com
```

Then follow steps 4 and 5 above, with the tunnel address in place of `$DOMAIN` and `-f compose.yaml -f compose.tunnel.yaml` on each command.

Limits:
- It's live only while that machine is on.
- The address changes whenever the tunnel container restarts.
- Cloudflare offers quick tunnels for testing, with no uptime guarantee.

Caddy takes the visitor's IP from Cloudflare's `Cf-Connecting-IP` header, but only for requests from a private address (the tunnel container). So per-IP login limits still see real visitors, and a direct client on a VM can't fake the header.

## Monitoring

`compose.monitoring.yaml` adds Prometheus and Grafana, with the same alert rules (`observability/alerts.yml`) and the same *Reliant overview* dashboard as local development.

```bash
# add to deploy/.env:  GRAFANA_ADMIN_PASSWORD=<openssl rand -base64 18>
docker compose -f compose.yaml [-f compose.tunnel.yaml] -f compose.monitoring.yaml up -d --wait
```

- **Prometheus** scrapes each process's internal `/metrics` port over the compose network, keeping 15 days.
- **Grafana** listens on `127.0.0.1:3003` only, never through Caddy or the tunnel, and requires the admin password. Anonymous access and sign-up are off.
- **From another machine**, use an SSH tunnel:
  ```bash
  ssh -L 3003:127.0.0.1:3003 <vm>
  ```
  then open http://localhost:3003.
- **Alerts** are evaluated and shown in Prometheus and Grafana. Nothing sends them anywhere yet; that would need an Alertmanager with a destination such as email or Slack.

## What's exposed, and what isn't

| Path | Goes to | Who uses it |
|---|---|---|
| `POST /events` | ingest | producers, with `INGEST_TOKEN` |
| `/demo-receiver/*` | demo receiver | Reliant's own deliveries |
| everything else | dashboard + its API | people (sign-in) and scripts (tenant API key) |

**Not exposed:**
- Postgres, Redis and Redpanda have no published ports, and neither does any `/metrics` port.
- Only Caddy listens on the host.

**Settings fixed in `compose.yaml` for public use:**
- `ALLOW_PRIVATE_ENDPOINTS=false`: endpoints can't point at the VM's internal network, cloud metadata or localhost. The check runs at registration and again on every connection.
- `COOKIE_SECURE=true`: the session cookie only travels over HTTPS.
- `TRUST_PROXY=true`: login rate limits use the real client IP from Caddy.
- A lower per-tenant ingest limit by default (50/s, burst 100).
- **No dev tenants.** `DEV_SEED` isn't set, so `tenant_a` / `tenant_b`, whose API keys are in the repo, never exist here.

**Caddy adds** HSTS, `nosniff`, `X-Frame-Options: DENY` and `Referrer-Policy: no-referrer`, and strips `Server`.

**Know before you open it up:**
- `INGEST_TOKEN` is one token for every producer, and the tenant comes from the request body. Anyone holding it can send events for any tenant. Share it only with producers you trust; per-tenant ingest keys aren't built.
- One VM is one failure domain: one Postgres, one broker, no replicas. Delivery guarantees hold across process and container restarts, not across losing the disk.

## Operating it

| Task | Command |
|---|---|
| Update | `git pull && docker compose up -d --build --wait` (migrate re-runs; it's idempotent) |
| Logs | `docker compose logs -f worker` (JSON lines) |
| Retention | `docker compose run --rm --no-deps ingest node retention.js --apply` from a daily cron. Archives go to `./archive` inside the container: mount a volume or set `ARCHIVE_BUCKET` to keep them. |
| Back up Postgres | `docker compose exec postgres pg_dump -U reliant reliant \| gzip > reliant-$(date +%F).sql.gz` |

## How this was checked

The whole stack was brought up locally with `DOMAIN=localhost`, so Caddy issued a local certificate. Then, through Caddy over HTTPS:
- `create-tenant` and `create-user` worked inside the image.
- Plain HTTP redirected to HTTPS, and the security headers were present.
- The published dev key was rejected.
- Sign-in set a `Secure`, `HttpOnly` cookie.
- An event sent to `/events` was verified and handled by the demo receiver, and the dashboard reported it delivered.
- With the production settings, registering cloud metadata, `localhost` or an internal service name was refused.

Two things weren't possible locally:
- **The demo receiver over a public HTTPS hop.** Locally the receiver was registered by its internal name, with the private-address guard switched off for that run only.
- **A real Let's Encrypt certificate.**

Neither has run on a real VM yet.
