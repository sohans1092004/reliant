// npm run migrate - applies schema.sql (idempotent), creates Kafka topics, and gives
// any endpoint without a signing secret a new one (never printed - rotate it in the dashboard).
import fs from 'node:fs/promises';
import { pool, encryptSecret, ensureTopics, kafka, TOPICS, EVENT_PARTITIONS } from './lib/infra.js';
import { generateSecret } from './core/signing.js';

await pool.query(await fs.readFile(new URL('./schema.sql', import.meta.url), 'utf8'));
console.log('Schema applied.');

const { rows } = await pool.query('SELECT endpoint_id, tenant_id, url FROM endpoints WHERE secret_encrypted IS NULL');
for (const ep of rows) {
  const secret = generateSecret();
  await pool.query('UPDATE endpoints SET secret_encrypted = $2 WHERE endpoint_id = $1', [ep.endpoint_id, encryptSecret(secret)]);
  // Never print the secret: in a cluster this runs as a Job whose logs are collected and kept.
  // The tenant gets a usable secret via the dashboard's "Rotate secret" (shown once).
  console.log(`Generated a signing secret for ${ep.tenant_id} endpoint ${ep.endpoint_id}; use "Rotate secret" in the dashboard to obtain one.`);
}

await ensureTopics();
console.log('Kafka topics ensured.');

// Adding partitions remaps tenants (key hash % partition count). An event still
// waiting on its OLD partition could then be overtaken by a newer one on the new
// partition, breaking per-tenant order. So only grow when nothing is waiting:
// stop ingest, let the consumer catch up, then migrate.
async function growPartitions() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const [topic] = (await admin.fetchTopicMetadata({ topics: [TOPICS.events] })).topics;
    const current = topic.partitions.length;
    if (current >= EVENT_PARTITIONS) return console.log(`${TOPICS.events}: ${current} partitions, nothing to do.`);
    const end = await admin.fetchTopicOffsets(TOPICS.events);
    const [{ partitions }] = await admin.fetchOffsets({ groupId: 'delivery-workers', topics: [TOPICS.events] });
    const lag = end.reduce((sum, p) => sum + Number(p.offset) - Math.max(Number(partitions.find((c) => c.partition === p.partition)?.offset ?? 0), 0), 0);
    if (lag > 0) {
      return console.log(`NOT growing ${TOPICS.events} from ${current} to ${EVENT_PARTITIONS} partitions: ${lag} messages are still unconsumed. ` +
        'Stop ingest, let the consumer catch up, then run migrate again.');
    }
    await admin.createPartitions({ topicPartitions: [{ topic: TOPICS.events, count: EVENT_PARTITIONS }] });
    console.log(`${TOPICS.events}: grew from ${current} to ${EVENT_PARTITIONS} partitions (lag was 0).`);
  } finally {
    await admin.disconnect();
  }
}
await growPartitions();
await pool.end();
