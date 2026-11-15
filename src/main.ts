import { buildApp } from './app.js';
import { pool } from './db.js';
import { startWorkers } from './worker.js';
import { shutdownTelemetry } from './telemetry.js';
import { createFanoutDispatcher } from './fanout.js';

function positiveInteger(value: string | undefined, fallback: number, name: string) {
  const number = Number(value ?? fallback);
  if (!Number.isInteger(number) || number < 1) throw new Error(`${name} must be a positive integer`);
  return number;
}
const port = positiveInteger(process.env.PORT, 3000, 'PORT');
const fanoutDispatcher = createFanoutDispatcher();
const app = buildApp({ testViewer: process.env.TEST_VIEWER_ENABLED === 'true', callbackOrigin: `http://127.0.0.1:${port}` });
app.log.info({ fanoutDispatcher: fanoutDispatcher.backend }, 'Fanout dispatcher configured');
await app.listen({ port, host: process.env.HOST ?? '127.0.0.1' });
const workers = startWorkers({
  intervalMs: positiveInteger(process.env.WORKER_INTERVAL_MS, 500, 'WORKER_INTERVAL_MS'),
  batchSize: positiveInteger(process.env.WORKER_BATCH_SIZE, 4, 'WORKER_BATCH_SIZE'),
  timeoutMs: positiveInteger(process.env.WEBHOOK_TIMEOUT_MS, 5000, 'WEBHOOK_TIMEOUT_MS'),
  leaseMs: positiveInteger(process.env.DELIVERY_LEASE_MS, 30000, 'DELIVERY_LEASE_MS'),
  logger: app.log,
  fanoutDispatcher,
});
let shuttingDown = false;
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  try {
    await workers.stop();
    await app.close();
    await pool.end();
    await shutdownTelemetry();
  } catch (error) {
    app.log.error({ err: error }, 'Graceful shutdown failed');
    process.exitCode = 1;
  }
}
process.on('SIGTERM', () => void shutdown());
process.on('SIGINT', () => void shutdown());
