import Fastify from 'fastify';
import { z } from 'zod';
import { randomUUID, createHash, timingSafeEqual } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { ingest, pool, getEventQueueCapacity, EventQueueFullError } from './db.js';
import { isWebhookSafe } from './url.js';
import { registerTestViewer } from './test-viewer.js';
import { eventSchema, subscriptionSchema } from './validation.js';
import { registry, httpRequests, httpDuration, backlog, oldestPending } from './metrics.js';

const uuid = z.string().uuid();
const pagination = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(50),
  offset: z.coerce.number().int().min(0).max(100000).default(0),
});
const deliveryColumns = `d.id,d.event_id,d.subscription_id,d.delivery_mode,d.status,d.attempt_count,
  d.next_attempt_at,d.final_reason,d.created_at,
  COALESCE((SELECT json_agg(a ORDER BY a.attempt_no) FROM
    (SELECT * FROM delivery_attempts WHERE delivery_id=d.id ORDER BY attempt_no DESC LIMIT 100) a),'[]'::json) AS attempts`;

export function buildApp(options: { testViewer?: boolean; callbackOrigin?: string; eventQueueCapacity?: number; apiToken?: string } = {}) {
  const capacity = getEventQueueCapacity(options.eventQueueCapacity);
  const token = options.apiToken ?? process.env.API_TOKEN;
  if (!token || token.length < 32) throw new Error('API_TOKEN must contain at least 32 characters');
  const tokenHash = createHash('sha256').update(token).digest();
  const starts = new WeakMap<object, number>();
  const app = Fastify({
    bodyLimit: 1024 * 1024,
    logger: {
      redact: ['req.headers.authorization', 'secret', 'callback_token'],
      serializers: {
        req: request => ({ method: request.method, url: request.url?.replace(/(\/test-viewer\/webhooks\/[^/]+\/)[^/?]+/, '$1[redacted]') }),
      },
    },
    genReqId: () => randomUUID(),
  });
  app.addHook('onRequest', async (request, reply) => {
    starts.set(request, performance.now());
    reply.header('X-Request-Id', request.id);
    const path = request.url.split('?')[0];
    const publicAsset = ['GET', 'HEAD'].includes(request.method) && ['/health/live', '/health/ready', '/test-viewer', '/test-viewer/', '/test-viewer/app.js'].includes(path);
    const signedCallback = request.method === 'POST' && path.startsWith('/test-viewer/webhooks/');
    if (!publicAsset && !signedCallback) {
      const auth = request.headers.authorization;
      if (!auth?.startsWith('Bearer ') || !timingSafeEqual(tokenHash, createHash('sha256').update(auth.slice(7)).digest())) {
        return reply.code(401).header('WWW-Authenticate', 'Bearer').send({ code: 'UNAUTHORIZED', error: 'Valid bearer token required' });
      }
    }
  });
  app.addHook('onResponse', async (request, reply) => {
    const route = request.routeOptions.url ?? 'unmatched';
    httpRequests.inc({ method: request.method, route, status: String(reply.statusCode) });
    httpDuration.observe({ method: request.method, route }, (performance.now() - (starts.get(request) ?? performance.now())) / 1000);
  });
  app.setErrorHandler((error, request, reply) => {
    request.log.error({ err: error }, 'Request failed');
    const code = typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' ? error.code : '';
    const message = error instanceof Error ? error.message : 'Unexpected request failure';
    const statusCode = typeof error === 'object' && error !== null && 'statusCode' in error && typeof error.statusCode === 'number' ? error.statusCode : undefined;
    const unavailable = ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', '57P01', '57P02', '57P03', '53000', '53100', '53200', '53300', '40001', '40P01', '55P03', '08000', '08003', '08006'].includes(code)
      || /timeout|connection terminated/i.test(message);
    const status = unavailable ? 503 : error instanceof z.ZodError ? 400 : statusCode && statusCode >= 400 && statusCode < 500 ? statusCode : 500;
    if (status === 503) reply.header('Retry-After', '5');
    reply.code(status).send({
      code: unavailable ? 'SERVICE_UNAVAILABLE' : status >= 500 ? 'INTERNAL_ERROR' : 'INVALID_REQUEST',
      error: status >= 500 ? 'Service could not complete the request' : message,
      requestId: request.id,
    });
  });
  if (options.testViewer) app.register(async scoped => registerTestViewer(scoped, options.callbackOrigin));
  app.get('/health/live', async () => ({ status: 'ok' }));
  app.get('/health/ready', async (_, reply) => {
    try { await pool.query('SELECT 1'); return { status: 'ready' }; }
    catch (error) { app.log.error({ err: error }, 'Readiness database check failed'); return reply.code(503).send({ status: 'not_ready' }); }
  });
  app.get('/metrics', async (_, reply) => {
    const result = await pool.query(`SELECT
      (SELECT count(*)::integer FROM fanout_jobs WHERE status <> 'done') AS events,
      (SELECT count(*)::integer FROM deliveries WHERE status='pending') AS deliveries,
      COALESCE((SELECT EXTRACT(EPOCH FROM now()-min(created_at)) FROM deliveries WHERE status='pending'),0)::float AS oldest`);
    backlog.set({ kind: 'fanout' }, result.rows[0].events);
    backlog.set({ kind: 'delivery' }, result.rows[0].deliveries);
    oldestPending.set(result.rows[0].oldest);
    return reply.type(registry.contentType).send(await registry.metrics());
  });
  app.post('/events', async (request, reply) => {
    const parsed = eventSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ code: 'INVALID_EVENT', error: parsed.error.issues });
    const key = request.headers['idempotency-key'];
    const producer = request.headers['x-producer-id'] ?? 'default';
    if (typeof key !== 'string' || !key.trim() || key.length > 256 || typeof producer !== 'string' || !producer.trim() || producer.length > 256) {
      return reply.code(400).send({ code: 'INVALID_HEADERS', error: 'Valid Idempotency-Key and producer ID (at most 256 characters) required' });
    }
    try {
      const event = await ingest(producer, key, parsed.data, capacity);
      request.log.info({ eventId: event.id }, 'Event accepted');
      return reply.code(202).send(event);
    } catch (error) {
      if (!(error instanceof EventQueueFullError)) throw error;
      request.log.warn({ queuedEvents: error.queuedEvents, capacity: error.capacity }, 'Event admission rejected: queue full');
      return reply.code(503).header('Retry-After', '5').send({
        code: 'EVENT_QUEUE_FULL', error: error.message, capacity: error.capacity, queuedEvents: error.queuedEvents,
      });
    }
  });
  app.post('/subscriptions', async (request, reply) => {
    const parsed = subscriptionSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });
    if (!(await isWebhookSafe(parsed.data.webhookUrl))) return reply.code(400).send({ error: 'Unsafe webhook URL' });
    const id = randomUUID(), s = parsed.data;
    await pool.query('INSERT INTO subscriptions(id,webhook_url,secret,delivery_mode,rules) VALUES($1,$2,$3,$4,$5)', [id, s.webhookUrl, s.secret, s.deliveryMode, s.rules]);
    return reply.code(201).send({ id, webhookUrl: s.webhookUrl, deliveryMode: s.deliveryMode, rules: s.rules });
  });
  app.get('/subscriptions', async request => {
    const page = pagination.parse(request.query);
    return (await pool.query('SELECT id,webhook_url,delivery_mode,rules,active,created_at FROM subscriptions ORDER BY created_at,id LIMIT $1 OFFSET $2', [page.limit, page.offset])).rows;
  });
  app.delete<{ Params: { id: string } }>('/subscriptions/:id', async (request, reply) => {
    const id = uuid.parse(request.params.id);
    const result = await pool.query('UPDATE subscriptions SET active=false WHERE id=$1', [id]);
    return result.rowCount ? { deleted: true } : reply.code(404).send({ error: 'Subscription not found' });
  });
  for (const kind of ['events', 'subscriptions'] as const) {
    app.get<{ Params: { id: string } }>(`/${kind}/:id/deliveries`, async (request, reply) => {
      const id = uuid.parse(request.params.id), page = pagination.parse(request.query);
      if (!(await pool.query(`SELECT id FROM ${kind} WHERE id=$1`, [id])).rowCount) return reply.code(404).send({ error: `${kind} not found` });
      const column = kind === 'events' ? 'event_id' : 'subscription_id';
      return (await pool.query(`SELECT ${deliveryColumns} FROM deliveries d WHERE d.${column}=$1 ORDER BY d.created_at,d.id LIMIT $2 OFFSET $3`,
        [id, page.limit, page.offset])).rows;
    });
  }
  app.get<{ Params: { id: string } }>('/deliveries/:id', async (request, reply) => {
    const row = (await pool.query(`SELECT ${deliveryColumns} FROM deliveries d WHERE d.id=$1`, [uuid.parse(request.params.id)])).rows[0];
    return row ?? reply.code(404).send({ error: 'Delivery not found' });
  });
  app.get<{ Params: { id: string } }>('/deliveries/:id/attempts', async (request, reply) => {
    const id = uuid.parse(request.params.id), page = pagination.parse(request.query);
    if (!(await pool.query('SELECT id FROM deliveries WHERE id=$1', [id])).rowCount) return reply.code(404).send({ error: 'Delivery not found' });
    return (await pool.query('SELECT * FROM delivery_attempts WHERE delivery_id=$1 ORDER BY attempt_no LIMIT $2 OFFSET $3', [id, page.limit, page.offset])).rows;
  });
  return app;
}
