// AWS clients: KMS for envelope encryption of endpoint signing secrets, S3 for retention
// archives. Configured the standard AWS way (AWS_REGION, credentials chain). Locally they
// talk to LocalStack via AWS_ENDPOINT_URL=http://localhost:4566, which the SDK reads itself.
import { KMSClient, GenerateDataKeyCommand, DecryptCommand } from '@aws-sdk/client-kms';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

let kms, s3;
const kmsClient = () => (kms ??= new KMSClient({}));
// LocalStack serves buckets by path (localhost:4566/bucket), not as bucket.host subdomains.
const s3Client = () => (s3 ??= new S3Client({ forcePathStyle: Boolean(process.env.AWS_ENDPOINT_URL) }));

// Bound into every wrapped data key: KMS refuses to unwrap it under any other context, so a
// data key wrapped for some other purpose with the same master key can't be used here.
const CONTEXT = { purpose: 'reliant-endpoint-secret' };

// A fresh 256-bit key, plus the same key encrypted ("wrapped") by the KMS master key.
export async function generateDataKey(keyId) {
  const r = await kmsClient().send(new GenerateDataKeyCommand({ KeyId: keyId, KeySpec: 'AES_256', EncryptionContext: CONTEXT }));
  return { plaintext: Buffer.from(r.Plaintext), wrapped: Buffer.from(r.CiphertextBlob) };
}

// KMS finds the master key from the wrapped blob itself.
export async function unwrapDataKey(wrapped) {
  const r = await kmsClient().send(new DecryptCommand({ CiphertextBlob: wrapped, EncryptionContext: CONTEXT }));
  return Buffer.from(r.Plaintext);
}

// With kmsKeyId, S3 encrypts the object at rest under that KMS key (SSE-KMS).
export async function putObject(bucket, key, body, kmsKeyId) {
  await s3Client().send(new PutObjectCommand({
    Bucket: bucket, Key: key, Body: body, ContentType: 'application/gzip',
    ...(kmsKeyId && { ServerSideEncryption: 'aws:kms', SSEKMSKeyId: kmsKeyId }),
  }));
  return `s3://${bucket}/${key}`;
}
