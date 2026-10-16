// Creates a tenant and prints its dashboard API key ONCE (only its sha256 is stored).
// Then make the tenant's first owner with create-user.
//   npm run create-tenant -- <tenant_id> "<name>"
import crypto from 'node:crypto';
import { pool } from './lib/infra.js';
import { sha256 } from './lib/http.js';

const [tenantId, name = tenantId] = process.argv.slice(2);
if (!/^[a-z0-9_]{2,64}$/.test(tenantId ?? '')) {
  console.error('usage: npm run create-tenant -- <tenant_id: a-z 0-9 _> ["<name>"]');
  process.exit(2);
}
const apiKey = crypto.randomBytes(32).toString('base64url');
try {
  await pool.query('INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $2, $3)', [tenantId, name, sha256(apiKey)]);
  console.log(`Created tenant ${tenantId}.`);
  console.log(`Dashboard API key (shown once): ${apiKey}`);
  console.log(`Next: npm run create-user -- ${tenantId} <email> owner`);
} catch (err) {
  console.error(err.code === '23505' ? `tenant ${tenantId} already exists` : err.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
