// Pure HMAC signing/verification + secret encryption at rest. Node's crypto only.
//
// Compatible with Standard Webhooks (standardwebhooks.com): receivers can verify with
// its official libraries. Headers sent with every delivery:
//   webhook-id:        event_id (doubles as the nonce - receivers dedupe on it)
//   webhook-timestamp: unix seconds
//   webhook-signature: "v1,<base64 hmac>" - space-separated, one per active secret
// The signed content is `${id}.${timestamp}.${body}`, so changing any of the three breaks it.
// The HMAC key is the base64-decoded part of the secret after "whsec_" (32 random bytes).
import crypto from 'node:crypto';

export const ROTATION_WINDOW_MS = 24 * 60 * 60 * 1000;

export const generateSecret = () => 'whsec_' + crypto.randomBytes(32).toString('base64');

// Strict: Node's base64 decoder skips characters it doesn't know, so without the check
// two different malformed secrets could silently decode to the same key.
function signingKey(secret) {
  const b64 = secret.startsWith('whsec_') ? secret.slice('whsec_'.length) : secret;
  if (!b64 || b64.length % 4 || !/^[A-Za-z0-9+/]+={0,2}$/.test(b64)) throw new Error('signing secret must be whsec_<base64>');
  return Buffer.from(b64, 'base64');
}

export function sign(secret, id, timestamp, body) {
  return 'v1,' + crypto.createHmac('sha256', signingKey(secret)).update(`${id}.${timestamp}.${body}`).digest('base64');
}

// During a rotation window we sign with BOTH secrets, so a receiver still holding
// the old one keeps verifying until they switch over.
export function signatureHeader(secrets, id, timestamp, body) {
  return secrets.map((s) => sign(s, id, timestamp, body)).join(' ');
}

// What a receiver runs. Rejects stale/future timestamps (replay protection) and
// accepts if any signature in the header matches any of the receiver's secrets.
export function verify(secrets, id, timestamp, body, header, nowSec, toleranceSec = 300) {
  const ts = Number(timestamp);
  if (!Number.isFinite(ts) || Math.abs(nowSec - ts) > toleranceSec) return false;
  const given = String(header).split(' ').map((s) => Buffer.from(s));
  return secrets.some((secret) => {
    const expected = Buffer.from(sign(secret, id, timestamp, body));
    return given.some((g) => g.length === expected.length && crypto.timingSafeEqual(g, expected));
  });
}

// Which secrets to sign with right now, given an endpoint row's (decrypted) secrets.
export function activeSecrets({ secret, secretPrevious, rotatedAt }, now) {
  const inWindow = secretPrevious && rotatedAt && now - new Date(rotatedAt).getTime() < ROTATION_WINDOW_MS;
  return inWindow ? [secret, secretPrevious] : [secret];
}

// AES-256-GCM. `key` is 32 bytes. Output: base64(iv | tag | ciphertext).
// ponytail: local key from env stands in for KMS; swap encrypt/decrypt for KMS calls in prod.
export function encrypt(plaintext, key) {
  const iv = crypto.randomBytes(12);
  const c = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ct = Buffer.concat([c.update(plaintext, 'utf8'), c.final()]);
  return Buffer.concat([iv, c.getAuthTag(), ct]).toString('base64');
}

export function decrypt(blob, key) {
  const buf = Buffer.from(blob, 'base64');
  const d = crypto.createDecipheriv('aes-256-gcm', key, buf.subarray(0, 12));
  d.setAuthTag(buf.subarray(12, 28));
  return Buffer.concat([d.update(buf.subarray(28)), d.final()]).toString('utf8');
}
