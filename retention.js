// Retention: archive, then delete, events past the replay window together with their
// ledger rows and finished delivery rows. Events with a delivery still pending are
// never touched.
//
//   npm run retention              dry run: report what would go
//   npm run retention -- --apply   archive + delete
//
// RETENTION_DAYS (default 35) must stay above the 30-day replay requirement (ADR-001).
// The consumer already sends Kafka messages whose events row is gone to the DLQ topic,
// so deleting archived events can't stall a partition.
//
// Archives go to S3 when ARCHIVE_BUCKET is set (encrypted at rest under KMS_KEY_ID if that's
// set too), otherwise to local files in ./archive.
//
// Also maintains the ledger's monthly partitions: creates the coming months, and drops
// a month once it's past the cutoff AND empty (its rows went out with their events).
import fs from 'node:fs/promises';
import path from 'node:path';
import zlib from 'node:zlib';
import { promisify } from 'node:util';
import { pathToFileURL } from 'node:url';
import { pool } from './lib/infra.js';
import { log } from './lib/log.js';
import { putObject } from './lib/aws.js';

const gzip = promisify(zlib.gzip);
const BATCH = 1000;

export async function runRetention({ days = 35, apply = false, archiveDir = 'archive', tenantLike = '%',
  bucket = process.env.ARCHIVE_BUCKET, kmsKeyId = process.env.KMS_KEY_ID } = {}) {
  if (days < 30) throw new Error('retention must be >= 30 days (spec: 30-day replay window)');
  const cutoff = `${days} days`;
  const eligible = `e.created_at < NOW() - $1::interval AND e.tenant_id LIKE $2
    AND NOT EXISTS (SELECT 1 FROM delivery_attempts da WHERE da.event_id = e.event_id AND da.status = 'pending')`;

  await pool.query('SELECT ensure_ledger_partitions()');
  const { rows: [def] } = await pool.query('SELECT EXISTS (SELECT 1 FROM delivery_ledger_default) AS used');
  if (def.used) log.warn('ledger rows in delivery_ledger_default: a month had no partition. Move them before creating it');

  if (!apply) {
    const { rows: [r] } = await pool.query(`SELECT count(*)::int AS n FROM events e WHERE ${eligible}`, [cutoff, tenantLike]);
    return { dryRun: true, events: r.n };
  }

  if (!bucket) await fs.mkdir(archiveDir, { recursive: true });
  let total = 0;
  const files = [];
  for (;;) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Lock the batch so a replay can't re-queue one of these events mid-deletion.
      const { rows: events } = await client.query(
        `SELECT e.* FROM events e WHERE ${eligible} ORDER BY e.created_at LIMIT ${BATCH} FOR UPDATE SKIP LOCKED`,
        [cutoff, tenantLike],
      );
      if (!events.length) { await client.query('ROLLBACK'); break; }
      const ids = events.map((e) => e.event_id);
      const { rows: ledger } = await client.query('SELECT * FROM delivery_ledger WHERE event_id = ANY($1::uuid[]) ORDER BY id', [ids]);
      const { rows: attempts } = await client.query('SELECT * FROM delivery_attempts WHERE event_id = ANY($1::uuid[]) ORDER BY id', [ids]);

      // Archive BEFORE deleting. If the delete then fails, the next run archives the
      // batch again: duplicates in the archive, never a gap.
      const lines = events.map((e) => JSON.stringify({
        event: e,
        ledger: ledger.filter((l) => l.event_id === e.event_id),
        delivery_attempts: attempts.filter((a) => a.event_id === e.event_id),
      }));
      const now = new Date().toISOString();
      const name = `events-${now.replace(/[:.]/g, '-')}-${total}.jsonl.gz`;
      const body = await gzip(lines.join('\n') + '\n');
      if (bucket) { // one prefix per day, so a lifecycle rule or a lookup can target a date
        files.push(await putObject(bucket, `events/${now.slice(0, 10)}/${name}`, body, kmsKeyId));
      } else {
        const file = path.join(archiveDir, name);
        await fs.writeFile(file, body);
        files.push(file);
      }

      await client.query('DELETE FROM delivery_ledger WHERE event_id = ANY($1::uuid[])', [ids]);
      await client.query('DELETE FROM delivery_attempts WHERE event_id = ANY($1::uuid[])', [ids]);
      await client.query('DELETE FROM events WHERE event_id = ANY($1::uuid[])', [ids]);
      await client.query('COMMIT');
      total += events.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }
  // Expired dashboard sessions: already refused at sign-in, this just stops them piling up.
  await pool.query('DELETE FROM sessions WHERE expires_at <= NOW()');
  return { dryRun: false, events: total, files, droppedPartitions: await dropOldPartitions(cutoff) };
}

// A whole month past the cutoff whose rows are all gone. Rows can only remain if their
// event is still pending (kept above) - then the month stays until it isn't.
async function dropOldPartitions(cutoff) {
  const { rows } = await pool.query(
    `SELECT c.relname AS name FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
     WHERE i.inhparent = 'delivery_ledger'::regclass AND c.relname ~ '^delivery_ledger_[0-9]{4}_[0-9]{2}$'
       AND (to_date(substr(c.relname, 17), 'YYYY_MM') + INTERVAL '1 month') <= (NOW() - $1::interval) AT TIME ZONE 'UTC'
     ORDER BY 1`,
    [cutoff],
  );
  const dropped = [];
  for (const { name } of rows) {
    const { rows: [r] } = await pool.query(`SELECT EXISTS (SELECT 1 FROM ${name}) AS used`); // name matched the regex above
    if (r.used) continue;
    await pool.query(`DROP TABLE ${name}`);
    dropped.push(name);
  }
  return dropped;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const apply = process.argv.includes('--apply');
  const days = Number(process.env.RETENTION_DAYS || 35);
  const result = await runRetention({ days, apply });
  console.log(result.dryRun
    ? `Dry run: ${result.events} event(s) older than ${days} days would be archived and deleted. Re-run with --apply.`
    : `Archived and deleted ${result.events} event(s) older than ${days} days into ${result.files.length} file(s). `
      + `Dropped ledger partitions: ${result.droppedPartitions.join(', ') || 'none'}.`);
  await pool.end();
}
