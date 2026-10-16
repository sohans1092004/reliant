// Creates a dashboard user and prints a generated password ONCE. This is how a tenant's
// first owner is made; after that, owners add users from the dashboard.
//   npm run create-user -- <tenant_id> <email> [owner|viewer]
import { pool } from './lib/infra.js';
import { EMAIL } from './lib/auth.js';
import { hashPassword, generatePassword } from './core/password.js';

const [tenantId, rawEmail, role = 'owner'] = process.argv.slice(2);
const email = String(rawEmail ?? '').trim().toLowerCase();
if (!tenantId || !EMAIL.test(email) || !['owner', 'viewer'].includes(role)) {
  console.error('usage: npm run create-user -- <tenant_id> <email> [owner|viewer]');
  process.exit(2);
}
const password = generatePassword();
try {
  await pool.query('INSERT INTO users (tenant_id, email, password_hash, role) VALUES ($1, $2, $3, $4)',
    [tenantId, email, await hashPassword(password), role]);
  console.log(`Created ${role} ${email} for tenant ${tenantId}.`);
  console.log(`Password (shown once, change it after signing in): ${password}`);
} catch (err) {
  console.error(err.code === '23505' ? 'that email already has an account'
    : err.code === '23503' ? `no tenant ${tenantId}` : err.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
