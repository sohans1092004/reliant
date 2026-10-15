// OpenTelemetry tracing (spec §12): one trace per event, across processes -
//   ingest -> kafka.publish -> (consumer) webhook.post -> ledger.write
//                           -> (worker, minutes or hours later) deliver.retry -> webhook.post -> ledger.write
// The trace context rides in Kafka message headers AND is stored in events.traceparent,
// so retries, pings and replays long after the Kafka message is gone still join the
// original trace.
//
// Off unless OTEL_EXPORTER_OTLP_ENDPOINT is set (e.g. http://localhost:4318 for the
// Jaeger in docker-compose). When off, every call below is a cheap no-op.
import { trace, context, propagation, SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { NodeTracerProvider, BatchSpanProcessor } from '@opentelemetry/sdk-trace-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

export { SpanKind };
export const tracer = trace.getTracer('reliant');
let provider = null;

export function initTracing(service) {
  if (!process.env.OTEL_EXPORTER_OTLP_ENDPOINT || provider) return;
  provider = new NodeTracerProvider({
    resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: `reliant-${service}` }),
    spanProcessors: [new BatchSpanProcessor(new OTLPTraceExporter())], // reads OTEL_EXPORTER_OTLP_ENDPOINT
  });
  provider.register(); // also installs the W3C traceparent propagator
}

export const shutdownTracing = () => provider?.shutdown();

// Run `fn(span)` inside a new active span; errors are recorded and re-thrown.
//   opts: { kind, attributes, links, parent } - parent is a Context (see fromTraceparent)
export function inSpan(name, opts, fn) {
  const { parent = context.active(), ...spanOpts } = opts;
  return tracer.startActiveSpan(name, spanOpts, parent, async (span) => {
    try {
      return await fn(span);
    } catch (err) {
      span.recordException(err);
      span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
      throw err;
    } finally {
      span.end();
    }
  });
}

// W3C traceparent string <-> Context
export const fromTraceparent = (traceparent) =>
  (traceparent ? propagation.extract(context.active(), { traceparent }) : context.active());

export function currentTraceparent() {
  const carrier = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent ?? null;
}
