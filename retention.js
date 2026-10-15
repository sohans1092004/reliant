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
import fs from 'node:fs/promises';
import path from 'node:path';
import zlib from 'node:zlib';
import { promisify } from 'node:util';
import { pathToFileURL } from 'node:url';
import { pool } from './lib/infra.js';

const gzip = promisify(zlib.gzip);
const BATCH = 1000;

export async function runRetention({ days = 35, apply = false, archiveDir = 'archive', tenantLike = '%' } = {}) {
  if (days < 30) throw new Error('retention must be >= 30 days (spec: 30-day replay window)');
  const cutoff = `${days} days`;
  const eligible = `e.created_at < NOW() - $1::interval AND e.tenant_id LIKE $2
    AND NOT EXISTS (SELECT 1 FROM delivery_attempts da WHERE da.event_id = e.event_id AND da.status = 'pending')`;

  if (!apply) {
    const { rows: [r] } = await pool.query(`SELECT count(*)::int AS n FROM events e WHERE ${eligible}`, [cutoff, tenantLike]);
    return { dryRun: true, events: r.n };
  }

  await fs.mkdir(archiveDir, { recursive: true });
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
      // ponytail: local gzip files stand in for S3; swap the writeFile for an S3 put.
      const lines = events.map((e) => JSON.stringify({
        event: e,
        ledger: ledger.filter((l) => l.event_id === e.event_id),
        delivery_attempts: attempts.filter((a) => a.event_id === e.event_id),
      }));
      const file = path.join(archiveDir, `events-${new Date().toISOString().replace(/[:.]/g, '-')}-${total}.jsonl.gz`);
      await fs.writeFile(file, await gzip(lines.join('\n') + '\n'));
      files.push(file);

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
  return { dryRun: false, events: total, files };
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const apply = process.argv.includes('--apply');
  const days = Number(process.env.RETENTION_DAYS || 35);
  const result = await runRetention({ days, apply });
  console.log(result.dryRun
    ? `Dry run: ${result.events} event(s) older than ${days} days would be archived and deleted. Re-run with --apply.`
    : `Archived and deleted ${result.events} event(s) older than ${days} days into ${result.files.length} file(s).`);
  await pool.end();
}
