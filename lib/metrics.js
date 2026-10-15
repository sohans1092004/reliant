// Prometheus metrics (spec §12). Every process serves its own /metrics on an INTERNAL
// port (METRICS_PORT), never on the public dashboard port: these numbers span tenants.
//   ingest 9100, consumer 9101, worker 9102, dashboard 9103, retry-publisher 9104
// METRICS_PORT=0 turns the endpoint off (e.g. a second worker on the same host).
import http from 'node:http';
import client from 'prom-client';
import { log } from './log.js';

const tenantNote = 'tenant label: fine for hundreds of tenants; drop it if tenant count gets large (cardinality)';

export const metrics = {
  ingestRequests: new client.Counter({
    name: 'reliant_ingest_requests_total', help: 'Ingest API requests by HTTP status', labelNames: ['status'],
  }),
  ingestDuration: new client.Histogram({
    name: 'reliant_ingest_duration_seconds', help: 'Ingest request duration (both durability boundaries)',
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
  }),
  attempts: new client.Counter({
    name: 'reliant_delivery_attempts_total',
    help: `Delivery attempts. source: inline (consumer) | retry (worker); outcome: delivered | failed | timeout. ${tenantNote}`,
    labelNames: ['source', 'outcome', 'tenant'],
  }),
  attemptDuration: new client.Histogram({
    name: 'reliant_delivery_attempt_duration_seconds', help: 'Duration of one HTTP delivery attempt', labelNames: ['source'],
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20],
  }),
  deliveryLatency: new client.Histogram({
    name: 'reliant_delivery_latency_seconds', help: 'Event accepted -> first delivery attempt started (spec NFR: p95 < 5s)',
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300],
  }),
  attemptNumber: new client.Histogram({
    name: 'reliant_retry_attempt_number', help: 'Attempt number of each worker attempt (retry distribution)',
    buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  }),
  deadLetters: new client.Counter({
    name: 'reliant_dead_letters_total', help: 'Deliveries moved to the DLQ, by reason', labelNames: ['reason'],
  }),
  pings: new client.Counter({
    name: 'reliant_endpoint_pings_total', help: 'reliant.ping probes (endpoint vs event problem), by result', labelNames: ['result'],
  }),
  breakerTransitions: new client.Counter({
    name: 'reliant_circuit_breaker_transitions_total', help: 'Circuit breaker state changes', labelNames: ['from', 'to'],
  }),
  consumerLag: new client.Gauge({
    name: 'reliant_consumer_lag', help: 'Messages behind the end of the partition after the last batch', labelNames: ['partition'],
  }),
  poisonMessages: new client.Counter({
    name: 'reliant_poison_messages_total', help: 'Unprocessable Kafka messages sent to the DLQ topic',
  }),
};

export function serveMetrics(defaultPort) {
  const port = Number(process.env.METRICS_PORT ?? defaultPort);
  if (!port) return null;
  client.collectDefaultMetrics(); // CPU, memory, event-loop lag, GC - the consumer is CPU-bound
  const server = http.createServer(async (req, res) => {
    if (req.url !== '/metrics') {
      res.writeHead(404);
      return res.end();
    }
    try {
      // Collect before writing the header: a gauge that queries Postgres can reject, and
      // the 500 below would then throw (headers already sent) and crash the process.
      const body = await client.register.metrics();
      res.writeHead(200, { 'Content-Type': client.register.contentType });
      res.end(body);
    } catch (err) {
      log.error('metrics collection failed', { err });
      res.writeHead(500);
      res.end();
    }
  });
  server.listen(port, () => log.info('metrics listening', { port }));
  server.on('error', (err) => log.error('metrics server failed', { port, err }));
  return server;
}

export { client };
