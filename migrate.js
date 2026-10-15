// npm run migrate - applies schema.sql (idempotent), creates Kafka topics, and gives
// any endpoint without a signing secret a new one (printed once, here only).
import fs from 'node:fs/promises';
import { pool, encryptSecret, ensureTopics } from './lib/infra.js';
import { generateSecret } from './core/signing.js';

await pool.query(await fs.readFile(new URL('./schema.sql', import.meta.url), 'utf8'));
console.log('Schema applied.');

const { rows } = await pool.query('SELECT endpoint_id, tenant_id, url FROM endpoints WHERE secret_encrypted IS NULL');
for (const ep of rows) {
  const secret = generateSecret();
  await pool.query('UPDATE endpoints SET secret_encrypted = $2 WHERE endpoint_id = $1', [ep.endpoint_id, encryptSecret(secret)]);
  console.log(`Signing secret for ${ep.tenant_id} ${ep.url}: ${secret}`);
}

await ensureTopics();
console.log('Kafka topics ensured.');
await pool.end();
