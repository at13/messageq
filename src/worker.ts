import { deliverOnce, type AttemptObservation } from './db.js';
import { deliveryAttempts, deliveryDuration } from './metrics.js';
import { createFanoutDispatcher, type FanoutDispatcher } from './fanout.js';

export type WorkerLogger = {
  info: (fields: Record<string, unknown>, message: string) => void;
  error: (fields: Record<string, unknown>, message: string) => void;
};
export type WorkerOptions = {
  intervalMs?: number;
  batchSize?: number;
  timeoutMs?: number;
  leaseMs?: number;
  logger?: WorkerLogger;
  fanoutDispatcher?: FanoutDispatcher;
};
export type Workers = { stop: () => Promise<void> };

/** One bounded, non-overlapping loop. stop() wakes idle sleep and drains in-flight work. */
export function startWorkers(options: WorkerOptions = {}): Workers {
  const intervalMs = options.intervalMs ?? Number(process.env.WORKER_INTERVAL_MS ?? 500);
  const batchSize = options.batchSize ?? 10;
  const timeoutMs = options.timeoutMs ?? Number(process.env.WEBHOOK_TIMEOUT_MS ?? 5000);
  const leaseMs = options.leaseMs ?? Math.max(30_000, timeoutMs + 1000);
  if (!Number.isSafeInteger(intervalMs) || intervalMs < 1 || intervalMs > 2_147_483_647) {
    throw new Error('worker intervalMs must be a positive timer-safe integer');
  }
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 1000) {
    throw new Error('worker batchSize must be between 1 and 1000');
  }
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2_147_483_647) {
    throw new Error('worker timeoutMs must be a positive timer-safe integer');
  }
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= timeoutMs) {
    throw new Error('worker leaseMs must be an integer exceeding timeoutMs');
  }
  const fanoutDispatcher = options.fanoutDispatcher ?? createFanoutDispatcher();
  let stopping = false;
  let wake: (() => void) | undefined;
  const observe = (attempt: AttemptObservation) => {
    try {
      deliveryAttempts.inc({ outcome: attempt.outcome, status: String(attempt.status ?? 'unknown') });
      deliveryDuration.observe({ outcome: attempt.outcome }, attempt.durationMs / 1000);
    } catch {}
    try {
      options.logger?.info({
        deliveryId: attempt.deliveryId, attempt: attempt.attempt, outcome: attempt.outcome,
        status: attempt.status, durationMs: attempt.durationMs, applied: attempt.applied,
      }, 'Webhook attempt completed');
    } catch {}
  };
  const done = (async () => {
    while (!stopping) {
      try {
        for (let i = 0; i < batchSize && !stopping; i++) {
          if (!await fanoutDispatcher.dispatchOnce()) break;
        }
        for (let i = 0; i < batchSize && !stopping; i++) {
          if (!await deliverOnce({ timeoutMs, leaseMs, onAttempt: observe })) break;
        }
      } catch {
        // Do not log raw errors, URLs, payloads or secrets.
        try { options.logger?.error({ outcome: 'worker_error' }, 'Worker cycle failed; durable work retained'); } catch {}
      }
      if (!stopping) {
        await new Promise<void>(resolve => {
          const timer = setTimeout(() => { wake = undefined; resolve(); }, intervalMs);
          wake = () => { clearTimeout(timer); wake = undefined; resolve(); };
        });
      }
    }
  })();
  return {
    stop: async () => {
      stopping = true;
      wake?.();
      await done;
    },
  };
}
