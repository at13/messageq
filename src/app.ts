import Fastify from 'fastify';
import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import { ingest, pool, getEventQueueCapacity, EventQueueFullError } from './db.js';
import type { DeliveryMode } from './types.js';
import { isWebhookSafe } from './url.js';
import { registerTestViewer } from './test-viewer.js';

const eventSchema = z.object({ type: z.string().min(1), source: z.string().min(1), payload: z.record(z.string(), z.unknown()) });
const subSchema = z.object({ webhookUrl: z.string().url(), secret: z.string().min(8), deliveryMode: z.enum(['at_most_once','at_least_once','exactly_once']).default('at_least_once'), rules: z.object({ type: z.string().optional(), source: z.string().optional(), payload: z.record(z.string(), z.unknown()).optional() }).default({}) });
export function buildApp(options: { testViewer?: boolean; callbackOrigin?: string; eventQueueCapacity?: number } = {}) {
  const capacity = getEventQueueCapacity(options.eventQueueCapacity);
  const app = Fastify({ logger: true });
  if (options.testViewer) {
    app.register(async scoped => registerTestViewer(scoped, options.callbackOrigin));
  }
  app.post('/events', async (request, reply) => {
    const parsed = eventSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });
    const key = String(request.headers['idempotency-key'] ?? '');
    if (!key) return reply.code(400).send({ error: 'Idempotency-Key is required' });
    try { return reply.code(202).send(await ingest(String(request.headers['x-producer-id'] ?? 'default'), key, parsed.data, capacity)); }
    catch (error) {
      if (error instanceof EventQueueFullError) {
        request.log.warn({ queuedEvents: error.queuedEvents, capacity: error.capacity }, 'Event admission rejected: queue full');
        return reply.code(503).header('Retry-After', '5').send({
          code: 'EVENT_QUEUE_FULL', error: error.message, capacity: error.capacity, queuedEvents: error.queuedEvents,
        });
      }
      const statusCode = error instanceof Error && 'statusCode' in error && typeof error.statusCode === 'number' ? error.statusCode : 500;
      if (statusCode >= 500) request.log.error({ err: error }, 'Event ingestion failed');
      return reply.code(statusCode).send({ error: error instanceof Error ? error.message : 'Unknown ingestion error' });
    }
  });
  app.post('/subscriptions', async (request, reply) => {
    const parsed = subSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });
    if (!(await isWebhookSafe(parsed.data.webhookUrl))) return reply.code(400).send({ error: 'unsafe webhook URL' });
    const id = randomUUID(); const s = parsed.data;
    await pool.query('INSERT INTO subscriptions(id,webhook_url,secret,delivery_mode,rules) VALUES($1,$2,$3,$4,$5)', [id, s.webhookUrl, s.secret, s.deliveryMode as DeliveryMode, s.rules]);
    return reply.code(201).send({ id, ...s });
  });
  app.get('/subscriptions', async () => (await pool.query('SELECT id,webhook_url,delivery_mode,rules,active,created_at FROM subscriptions ORDER BY created_at')).rows);
  app.delete('/subscriptions/:id', async (request: any, reply) => { const result = await pool.query('UPDATE subscriptions SET active=false WHERE id=$1', [request.params.id]); return result.rowCount ? { deleted: true } : reply.code(404).send({ error: 'not found' }); });
  app.get('/events/:id/deliveries', async (request: any) => (await pool.query('SELECT * FROM deliveries WHERE event_id=$1 ORDER BY created_at', [request.params.id])).rows);
  app.get('/subscriptions/:id/deliveries', async (request: any) => (await pool.query('SELECT * FROM deliveries WHERE subscription_id=$1 ORDER BY created_at', [request.params.id])).rows);
  app.get('/deliveries/:id/attempts', async (request: any) => (await pool.query('SELECT * FROM delivery_attempts WHERE delivery_id=$1 ORDER BY attempt_no', [request.params.id])).rows);
  return app;
}
