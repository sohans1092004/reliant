// Password hashing for dashboard users: scrypt from Node's crypto, no dependency.
// Stored as "scrypt$<N>$<r>$<p>$<salt>$<hash>", so the cost can be raised later and old
// hashes still verify with the parameters they were made with.
import crypto from 'node:crypto';
import { promisify } from 'node:util';

const scrypt = promisify(crypto.scrypt);
// N=2^15, r=8: ~32 MB and tens of milliseconds per hash. Slow on purpose: a stolen
// users table costs an attacker that much per guess.
const COST = { N: 32768, r: 8, p: 1 };
const KEY_LEN = 32;
const maxmem = (N, r) => 256 * N * r; // scrypt needs 128*N*r bytes; Node's default cap is 32 MB

export const MIN_PASSWORD_LENGTH = 12;
const MAX_PASSWORD_LENGTH = 200; // scrypt is slow per byte too: don't let a 1 MB "password" in

// null = acceptable, else the reason. Length over composition rules (NIST 800-63B).
export function passwordProblem(password, email = '') {
  if (typeof password !== 'string') return 'password must be a string';
  if (password.length < MIN_PASSWORD_LENGTH) return `password must be at least ${MIN_PASSWORD_LENGTH} characters`;
  if (password.length > MAX_PASSWORD_LENGTH) return `password must be at most ${MAX_PASSWORD_LENGTH} characters`;
  if (email && password.toLowerCase().includes(email.toLowerCase().split('@')[0])) return 'password must not contain your email name';
  return null;
}

export async function hashPassword(password) {
  const salt = crypto.randomBytes(16);
  const hash = await scrypt(password, salt, KEY_LEN, { ...COST, maxmem: maxmem(COST.N, COST.r) });
  return ['scrypt', COST.N, COST.r, COST.p, salt.toString('base64'), hash.toString('base64')].join('$');
}

export async function verifyPassword(password, stored) {
  const [scheme, N, r, p, salt, hash] = String(stored).split('$');
  if (scheme !== 'scrypt' || !salt || !hash || typeof password !== 'string' || password.length > MAX_PASSWORD_LENGTH) return false;
  const expected = Buffer.from(hash, 'base64');
  const actual = await scrypt(password, Buffer.from(salt, 'base64'), expected.length,
    { N: Number(N), r: Number(r), p: Number(p), maxmem: maxmem(Number(N), Number(r)) });
  return crypto.timingSafeEqual(actual, expected);
}

// A readable random password for new users, shown to the owner once (like signing secrets).
export const generatePassword = () => crypto.randomBytes(18).toString('base64url'); // 24 chars, 144 bits
