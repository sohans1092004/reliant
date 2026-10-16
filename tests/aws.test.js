// KMS envelope encryption of endpoint secrets and S3 retention archives, against LocalStack.
// Runs only when AWS_ENDPOINT_URL is set (CI sets it):
//   docker compose --profile aws up -d
//   AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=test \
//   AWS_SECRET_ACCESS_KEY=test KMS_KEY_ID=alias/reliant-secrets node --test tests/aws.test.js
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { KMSClient, DecryptCommand } from '@aws-sdk/client-kms';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { pool, encryptSecret, decryptSecret, rewrapSecrets } from '../lib/infra.js';
import { encrypt, generateSecret } from '../core/signing.js';
import { runRetention } from '../retention.js';

const skip = !process.env.AWS_ENDPOINT_URL && 'AWS_ENDPOINT_URL not set (needs LocalStack)';
const T = 'aws_test';
const BUCKET = process.env.ARCHIVE_BUCKET || 'reliant-archive';

async function cleanup() {
  await pool.query('DELETE FROM delivery_ledger WHERE tenant_id = $1', [T]);
  await pool.query('DELETE FROM events WHERE tenant_id = $1', [T]);
  await pool.query('DELETE FROM endpoints WHERE tenant_id = $1', [T]);
  await pool.query('DELETE FROM tenants WHERE tenant_id = $1', [T]);
}

before(async () => {
  if (skip) return;
  assert.ok(process.env.KMS_KEY_ID, 'KMS_KEY_ID must be set too');
  await cleanup();
  await pool.query('INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES ($1, $1, $2)', [T, crypto.randomUUID()]);
});

after(async () => {
  if (!skip) await cleanup();
  await pool.end();
});

test('KMS envelope: secrets round-trip; ciphertext never holds the secret or reuses a data key', { skip }, async () => {
  const secret = generateSecret();
  const [a, b] = [await encryptSecret(secret), await encryptSecret(secret)];
  assert.match(a, /^kms1:[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$/);
  assert.ok(!a.includes(secret.slice(6)));
  assert.notEqual(a.split(':')[1], b.split(':')[1], 'each secret must get its own data key');
  assert.equal(await decryptSecret(a), secret);
  assert.equal(await decryptSecret(b), secret);
});

test('KMS envelope: a tampered wrapped key or the wrong encryption context is refused', { skip }, async () => {
  const blob = await encryptSecret(generateSecret());
  const [, wrapped, data] = blob.split(':');
  const flipped = Buffer.from(wrapped, 'base64');
  flipped[flipped.length - 5] ^= 1;
  await assert.rejects(decryptSecret(`kms1:${flipped.toString('base64')}:${data}`));
  // The data key is bound to its purpose: unwrapping it under another context fails.
  await assert.rejects(new KMSClient({}).send(new DecryptCommand({
    CiphertextBlob: Buffer.from(wrapped, 'base64'), EncryptionContext: { purpose: 'something-else' } })));
});

test('rewrapSecrets moves env-key secrets to KMS and leaves KMS ones alone', { skip }, async () => {
  const key = Buffer.from(process.env.SECRET_ENCRYPTION_KEY, 'base64');
  const [cur, prev] = [generateSecret(), generateSecret()];
  const { rows: [ep] } = await pool.query(
    `INSERT INTO endpoints (tenant_id, url, secret_encrypted, secret_previous_encrypted, secret_rotated_at)
     VALUES ($1, 'https://example.com', $2, $3, NOW()) RETURNING endpoint_id`, [T, encrypt(cur, key), encrypt(prev, key)]);

  assert.equal(await rewrapSecrets({ tenantLike: T }), 1);
  const { rows: [after] } = await pool.query('SELECT secret_encrypted, secret_previous_encrypted FROM endpoints WHERE endpoint_id = $1', [ep.endpoint_id]);
  assert.ok(after.secret_encrypted.startsWith('kms1:') && after.secret_previous_encrypted.startsWith('kms1:'));
  assert.deepEqual([await decryptSecret(after.secret_encrypted), await decryptSecret(after.secret_previous_encrypted)], [cur, prev]);
  assert.equal(await rewrapSecrets({ tenantLike: T }), 0, 'second run has nothing to do');
});

test('retention archives to S3, encrypted under KMS, before deleting', { skip }, async () => {
  const id = crypto.randomUUID();
  await pool.query(`INSERT INTO events (event_id, tenant_id, type, payload, created_at) VALUES ($1, $2, 't', '{"k":1}', NOW() - INTERVAL '40 days')`, [id, T]);

  const r = await runRetention({ days: 35, apply: true, tenantLike: T, bucket: BUCKET, kmsKeyId: process.env.KMS_KEY_ID });
  assert.equal(r.events, 1);
  assert.match(r.files[0], new RegExp(`^s3://${BUCKET}/events/\\d{4}-\\d{2}-\\d{2}/events-.*\\.jsonl\\.gz$`));

  const obj = await new S3Client({ forcePathStyle: true }).send(new GetObjectCommand({ Bucket: BUCKET, Key: r.files[0].slice(`s3://${BUCKET}/`.length) }));
  assert.equal(obj.ServerSideEncryption, 'aws:kms');
  const line = JSON.parse(zlib.gunzipSync(Buffer.from(await obj.Body.transformToByteArray())).toString());
  assert.equal(line.event.event_id, id);
  assert.equal((await pool.query('SELECT 1 FROM events WHERE event_id = $1', [id])).rowCount, 0);
});
