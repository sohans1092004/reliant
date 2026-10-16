// Dashboard authentication. Two ways in, one result: { tenantId, role, via, userId?, email? }.
//   - Authorization: Bearer <tenant API key>  for scripts; acts as the tenant's owner
//   - the reliant_session cookie              for people, after email/password login
// Either way the tenant comes from the credential, never from the request (tenant isolation).
import crypto from 'node:crypto';
import { pool } from './infra.js';
import { bearer, sha256 } from './http.js';
import { takeToken } from './rate-limit.js';
import { hashPassword, verifyPassword } from '../core/password.js';

const COOKIE = 'reliant_session';
export const SESSION_HOURS = 12;
// Secure unless explicitly off: browsers treat http://localhost as secure, so dev works as is.
const SECURE = process.env.COOKIE_SECURE !== 'false';
export const EMAIL = /^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{1,60}$/;

// HttpOnly: page scripts (an XSS) can't read it. SameSite=Strict: not sent on requests that
// other sites start.
export const sessionCookie = (token, maxAgeSec = SESSION_HOURS * 3600) =>
  `${COOKIE}=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAgeSec}${SECURE ? '; Secure' : ''}`;
export const clearedCookie = () => sessionCookie('', 0);

function sessionToken(req) {
  for (const part of (req.headers.cookie || '').split(';')) {
    const [name, ...value] = part.trim().split('=');
    if (name === COOKIE) return value.join('=');
  }
  return null;
}

export async function authenticate(req) {
  const key = bearer(req);
  if (key) {
    const { rows: [t] } = await pool.query('SELECT tenant_id FROM tenants WHERE api_key_hash = $1', [sha256(key)]);
    return t ? { tenantId: t.tenant_id, role: 'owner', via: 'api_key' } : null;
  }
  const token = sessionToken(req);
  if (!token) return null;
  const { rows: [s] } = await pool.query(
    `SELECT u.user_id, u.tenant_id, u.email, u.role FROM sessions s JOIN users u USING (user_id)
     WHERE s.token_hash = $1 AND s.expires_at > NOW()`, [sha256(token)]);
  return s ? { tenantId: s.tenant_id, role: s.role, via: 'session', userId: s.user_id, email: s.email, tokenHash: sha256(token) } : null;
}

// Cookie-authenticated writes must be same-origin JSON. SameSite=Strict already keeps the
// cookie off cross-site requests in current browsers; these checks don't depend on it:
//  - a page on another site can't send Content-Type: application/json without a CORS
//    preflight, which this server never approves
//  - browsers name the page that started a request in Origin, and it must be this host
export function sameOriginJson(req) {
  if (!(req.headers['content-type'] || '').startsWith('application/json')) return false;
  const origin = req.headers.origin;
  if (!origin) return true; // not sent by every browser on same-origin requests
  try {
    return new URL(origin).host === req.headers.host;
  } catch {
    return false; // e.g. "null" from a sandboxed frame
  }
}

export async function createSession(userId) {
  const token = crypto.randomBytes(32).toString('base64url');
  await pool.query(`INSERT INTO sessions (token_hash, user_id, expires_at) VALUES ($1, $2, NOW() + $3 * INTERVAL '1 hour')`,
    [sha256(token), userId, SESSION_HOURS]);
  await pool.query('DELETE FROM sessions WHERE user_id = $1 AND expires_at <= NOW()', [userId]);
  return token;
}

let dummyHash;
// Returns a session token, or null for a wrong email or password (the caller can't tell
// which, and neither can an attacker timing it: an unknown email is still checked against
// a hash). Throws 429 once the email or the IP has used up its attempts.
export async function login(email, password, ip) {
  const e = String(email ?? '').trim().toLowerCase();
  if (!EMAIL.test(e)) return null;
  // Per email: 5 attempts, then 1 a minute (guessing one account). Per IP: 30, then 1 every
  // 10s (spraying many accounts). Fails open if Redis is down, like ingest's limiter.
  const allowed = (await takeToken(`login:email:${e}`, 1 / 60, 5)) && (await takeToken(`login:ip:${ip}`, 0.1, 30));
  if (!allowed) throw Object.assign(new Error('too many login attempts, try again in a minute'), { status: 429 });
  const { rows: [u] } = await pool.query('SELECT user_id, password_hash FROM users WHERE lower(email) = $1', [e]);
  dummyHash ??= await hashPassword(crypto.randomBytes(16).toString('hex'));
  const ok = await verifyPassword(String(password ?? ''), u?.password_hash ?? dummyHash);
  return u && ok ? createSession(u.user_id) : null;
}
