// Graceful shutdown (spec failure scenario #10) for every process. In a container Node
// runs as PID 1, where an unhandled SIGTERM is ignored - Kubernetes would then wait out
// its 30s grace period and SIGKILL mid-delivery. `cleanup` should stop taking new work
// and let in-flight work (and its ledger writes) finish.
import { log } from './log.js';
import { shutdownTracing } from './tracing.js';

const FORCE_EXIT_MS = 25_000; // under Kubernetes' default 30s terminationGracePeriodSeconds

export function onShutdown(cleanup) {
  let stopping = false;
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, async () => {
      if (stopping) return;
      stopping = true;
      log.info('shutting down', { signal });
      setTimeout(() => {
        // Anything unfinished is safe to abandon: unresolved Kafka offsets are redelivered,
        // and a worker's leased row is picked up again once its lease expires.
        log.error('shutdown took too long, forcing exit');
        process.exit(1);
      }, FORCE_EXIT_MS).unref();
      try {
        await cleanup();
        await shutdownTracing(); // flush buffered spans
        log.info('shutdown complete');
        process.exit(0);
      } catch (err) {
        log.error('shutdown failed', { err });
        process.exit(1);
      }
    });
  }
}
