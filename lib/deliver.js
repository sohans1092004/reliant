// One HTTP delivery attempt: SSRF check -> sign -> POST -> ledger row -> breaker update.
// Shared by consumer.js (inline attempt) and delivery-worker.js (retries).
import crypto from 'node:crypto';
import dns from 'node:dns/promises';
import net from 'node:net';
import { pool, decryptSecret } from './infra.js';
import { recordOutcome } from './breaker.js';
import { activeSecrets, signatureHeader } from '../core/signing.js';

// Private/internal ranges a customer URL must never resolve to.
const blocked = new net.BlockList();
for (const [ip, bits] of [['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8],
  ['169.254.0.0', 16], ['172.16.0.0', 12], ['192.168.0.0', 16], ['224.0.0.0', 4], ['240.0.0.0', 4]]) {
  blocked.addSubnet(ip, bits, 'ipv4');
}
for (const [ip, bits] of [['::', 128], ['::1', 128], ['fc00::', 7], ['fe80::', 10], ['ff00::', 8]]) {
  blocked.addSubnet(ip, bits, 'ipv6');
}

// Only for local dev, where test endpoints live on localhost.
const ALLOW_PRIVATE = process.env.ALLOW_PRIVATE_ENDPOINTS === 'true';

// Checked at delivery time, not just registration: DNS can be re-pointed later.
// ponytail: fetch re-resolves the name after this check (DNS-rebinding window).
// Close it by pinning the resolved IP with a custom undici dispatcher.
export async function assertSafeUrl(url) {
  const u = new URL(url);
  if (u.protocol !== 'https:' && u.protocol !== 'http:') throw new Error(`blocked: protocol ${u.protocol}`);
  if (ALLOW_PRIVATE) return;
  const addrs = await dns.lookup(u.hostname.replace(/^\[|\]$/g, ''), { all: true });
  for (const { address, family } of addrs) {
    if (blocked.check(address, family === 6 ? 'ipv6' : 'ipv4')) throw new Error(`blocked: ${u.hostname} resolves to private address ${address}`);
  }
}

// Signed POST of `event` to `endpoint`. No recording - callers decide what to record.
// endpoint: { endpoint_id, url, secret_encrypted, secret_previous_encrypted, secret_rotated_at }
async function post(event, endpoint, timeoutMs) {
  const body = JSON.stringify({
    event_id: event.event_id, tenant_id: event.tenant_id, type: event.type,
    payload: event.payload, created_at: event.created_at,
  });
  try {
    if (!endpoint.secret_encrypted) throw new Error('endpoint has no signing secret (run npm run migrate)');
    await assertSafeUrl(endpoint.url);
    const timestamp = Math.floor(Date.now() / 1000);
    const secrets = activeSecrets({
      secret: decryptSecret(endpoint.secret_encrypted),
      secretPrevious: endpoint.secret_previous_encrypted && decryptSecret(endpoint.secret_previous_encrypted),
      rotatedAt: endpoint.secret_rotated_at,
    }, Date.now());

    const response = await fetch(endpoint.url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'webhook-id': event.event_id,
        'webhook-timestamp': String(timestamp),
        'webhook-signature': signatureHeader(secrets, event.event_id, timestamp, body),
      },
      body,
      redirect: 'manual', // a redirect could point at internal infra - treat 3xx as failure
      signal: AbortSignal.timeout(timeoutMs),
    });
    // fetch only throws on network errors - a 500 still "succeeds" here.
    return { ok: response.ok, httpStatus: response.status, error: response.ok ? null : `HTTP ${response.status}` };
  } catch (err) {
    return { ok: false, httpStatus: null, error: err.name === 'TimeoutError' ? `timed out after ${timeoutMs}ms` : err.message };
  }
}

const recordBreaker = (endpointId, ok) =>
  recordOutcome(endpointId, ok).catch((err) => console.error('Breaker update failed:', err.message));

// event: { event_id, tenant_id, type, payload, created_at }
// Returns { ok, httpStatus, error }. Throws only if the ledger write fails - callers
// must then NOT treat the attempt as recorded (no offset commit / no row update).
export async function attemptDelivery(event, endpoint, attemptNumber, timeoutMs) {
  const started = Date.now();
  const result = await post(event, endpoint, timeoutMs);
  await pool.query(
    `INSERT INTO delivery_ledger (event_id, endpoint_id, tenant_id, attempt_number, status, http_status_code, latency_ms, error_detail)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [event.event_id, endpoint.endpoint_id, event.tenant_id, attemptNumber, result.ok ? 'delivered' : 'failed', result.httpStatus, Date.now() - started, result.error],
  );
  await recordBreaker(endpoint.endpoint_id, result.ok);
  return result;
}

// Health probe: a signed `reliant.ping` with a fresh id and an empty payload. Receivers
// are told to 2xx it and do nothing else. Answers "is the endpoint itself working?" so
// the worker can tell a broken endpoint from one event the endpoint keeps rejecting.
// Not a ledger row (it isn't an event); it does feed the breaker - a healthy ping is
// real evidence the endpoint is up.
export async function pingEndpoint(endpoint, tenantId, timeoutMs) {
  const ping = {
    event_id: crypto.randomUUID(), tenant_id: tenantId, type: 'reliant.ping',
    payload: {}, created_at: new Date().toISOString(),
  };
  const result = await post(ping, endpoint, timeoutMs);
  await recordBreaker(endpoint.endpoint_id, result.ok);
  return result;
}
