// Structured logs: one JSON object per line, so a log pipeline can filter by field.
// Pass event_id / endpoint_id / tenant_id wherever they're known - that's what lets
// one event be followed from ingest through the consumer, the worker and the DLQ.
//   log.info('delivered', { event_id, endpoint_id, http_status: 200 })
const service = process.argv[1]?.split(/[\\/]/).pop()?.replace(/\.js$/, '') ?? 'reliant';

function write(level, msg, fields = {}) {
  const line = { ts: new Date().toISOString(), level, service, msg, ...fields };
  // A refused connection is an AggregateError with an empty message; its code says what happened.
  if (line.err instanceof Error) line.err = line.err.message || line.err.code || line.err.name;
  (level === 'error' ? process.stderr : process.stdout).write(JSON.stringify(line) + '\n');
}

export const log = {
  info: (msg, fields) => write('info', msg, fields),
  warn: (msg, fields) => write('warn', msg, fields),
  error: (msg, fields) => write('error', msg, fields),
};
