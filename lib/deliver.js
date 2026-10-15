// One HTTP delivery attempt: SSRF check -> sign -> POST -> ledger row -> breaker update.
// Shared by consumer.js (inline attempt) and delivery-worker.js (retries).
import crypto from 'node:crypto';
import dns from 'node:dns/promises';
import dnsCallback from 'node:dns';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import { pool, decryptSecret } from './infra.js';
import { recordOutcome } from './breaker.js';
import { log } from './log.js';
import { inSpan, SpanKind } from './tracing.js';
import { SpanStatusCode } from '@opentelemetry/api';
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

const isBlocked = (address, family) => blocked.check(address, family === 6 || family === 'IPv6' ? 'ipv6' : 'ipv4');

// Protocol + address check. Used at registration, and before every delivery for IP-literal
// URLs (http://10.0.0.1/...), which never go through a DNS lookup at all.
export async function assertSafeUrl(url) {
  const u = new URL(url);
  if (u.protocol !== 'https:' && u.protocol !== 'http:') throw new Error(`blocked: protocol ${u.protocol}`);
  if (ALLOW_PRIVATE) return;
  const addrs = await dns.lookup(u.hostname.replace(/^\[|\]$/g, ''), { all: true });
  for (const { address, family } of addrs) {
    if (isBlocked(address, family)) throw new Error(`blocked: ${u.hostname} resolves to private address ${address}`);
  }
}

// The connection's OWN lookup. Checking a separate lookup first and then letting the
// HTTP client resolve again left a DNS-rebinding window: public IP for the check,
// internal IP for the connection. Here the address that's checked is the address the
// socket connects to.
export function safeLookup(hostname, options, callback) {
  dnsCallback.lookup(hostname, options, (err, address, family) => {
    if (err) return callback(err);
    const list = Array.isArray(address) ? address : [{ address, family }]; // `all: true` when Node races IPv4/IPv6
    const bad = list.find((a) => isBlocked(a.address, a.family));
    if (bad) return callback(new Error(`blocked: ${hostname} resolves to private address ${bad.address}`));
    callback(null, address, family);
  });
}

// POST with node:http(s) instead of fetch, because fetch can't take a custom lookup
// without adding the undici package. Redirects aren't followed (a 3xx could point at
// internal infra), so they count as failures. Resolves with the status code.
function post(url, headers, body, timeoutMs) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = (u.protocol === 'https:' ? https : http).request(u, {
      method: 'POST',
      headers: { ...headers, 'Content-Length': Buffer.byteLength(body) },
      lookup: ALLOW_PRIVATE ? undefined : safeLookup,
      signal: AbortSignal.timeout(timeoutMs),
    }, (res) => {
      res.resume(); // drain the body so the keep-alive socket can be reused
      res.on('end', () => resolve(res.statusCode));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end(body);
  });
}

// Signed POST of `event` to `endpoint`. No recording - callers decide what to record.
// endpoint: { endpoint_id, url, secret_encrypted, secret_previous_encrypted, secret_rotated_at }
// Traced: one CLIENT span per HTTP attempt, a child of whatever span is active (the
// event's trace). Only the host is recorded - customer URLs can carry tokens in the path.
export function sendSigned(event, endpoint, timeoutMs) {
  return inSpan('webhook.post', {
    kind: SpanKind.CLIENT,
    attributes: {
      'server.address': safeHost(endpoint.url), 'reliant.event_id': event.event_id,
      'reliant.endpoint_id': endpoint.endpoint_id, 'reliant.event_type': event.type,
    },
  }, async (span) => {
    const result = await postSigned(event, endpoint, timeoutMs);
    if (result.httpStatus) span.setAttribute('http.response.status_code', result.httpStatus);
    if (!result.ok) span.setStatus({ code: SpanStatusCode.ERROR, message: result.error });
    return result;
  });
}

const safeHost = (url) => { try { return new URL(url).host; } catch { return 'invalid'; } };

async function postSigned(event, endpoint, timeoutMs) {
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

    const status = await post(endpoint.url, {
      'Content-Type': 'application/json',
      'webhook-id': event.event_id,
      'webhook-timestamp': String(timestamp),
      'webhook-signature': signatureHeader(secrets, event.event_id, timestamp, body),
    }, body, timeoutMs);
    const ok = status >= 200 && status < 300;
    return { ok, httpStatus: status, error: ok ? null : `HTTP ${status}` };
  } catch (err) {
    const timedOut = err.name === 'AbortError'; // the only abort signal here is the timeout
    return { ok: false, httpStatus: null, timedOut, error: timedOut ? `timed out after ${timeoutMs}ms` : err.message };
  }
}

// Append attempts to the ledger in ONE statement. rows: { event_id, endpoint_id,
// tenant_id, attempt_number, ok, httpStatus, latencyMs, error }
// links: span contexts of the events' traces, so a batched write is findable from each.
export async function recordAttempts(rows, links = []) {
  if (!rows.length) return;
  const col = (f) => rows.map(f);
  await inSpan('ledger.write', { attributes: { 'reliant.rows': rows.length }, links }, () => pool.query(
    `INSERT INTO delivery_ledger (event_id, endpoint_id, tenant_id, attempt_number, status, http_status_code, latency_ms, error_detail)
     SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::int[], $5::text[], $6::int[], $7::int[], $8::text[])`,
    [col((r) => r.event_id), col((r) => r.endpoint_id), col((r) => r.tenant_id), col((r) => r.attempt_number),
      col((r) => (r.ok ? 'delivered' : 'failed')), col((r) => r.httpStatus), col((r) => r.latencyMs), col((r) => r.error)],
  ));
}

const recordBreaker = (endpointId, ok) =>
  recordOutcome(endpointId, ok).catch((err) => log.error('breaker update failed', { endpoint_id: endpointId, err }));

// event: { event_id, tenant_id, type, payload, created_at }
// Returns { ok, httpStatus, error }. Throws only if the ledger write fails - callers
// must then NOT treat the attempt as recorded (no offset commit / no row update).
export async function attemptDelivery(event, endpoint, attemptNumber, timeoutMs) {
  const started = Date.now();
  const result = await sendSigned(event, endpoint, timeoutMs);
  await recordAttempts([{ event_id: event.event_id, endpoint_id: endpoint.endpoint_id, tenant_id: event.tenant_id,
    attempt_number: attemptNumber, ...result, latencyMs: Date.now() - started }]);
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
  const result = await sendSigned(ping, endpoint, timeoutMs);
  await recordBreaker(endpoint.endpoint_id, result.ok);
  return result;
}
