import { buildApp } from './app.js';
import { deliverOnce, fanoutOnce, migrate } from './db.js';
await migrate();
const port = Number(process.env.PORT ?? 3000);
const app = buildApp({ testViewer: process.env.TEST_VIEWER_ENABLED === 'true', callbackOrigin: `http://127.0.0.1:${port}` });
await app.listen({ port, host: process.env.HOST ?? '0.0.0.0' });
const interval = setInterval(async () => { try { await fanoutOnce(); await deliverOnce(); } catch (error) { app.log.error(error); } }, Number(process.env.WORKER_INTERVAL_MS ?? 500));
const shutdown = async () => { clearInterval(interval); await app.close(); process.exit(0); };
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
