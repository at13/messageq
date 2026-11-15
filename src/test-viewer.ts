import type { FastifyInstance, FastifyRequest } from 'fastify';
import { readFile } from 'node:fs/promises';
import { randomUUID, randomBytes, createHmac, timingSafeEqual } from 'node:crypto';
import { z } from 'zod';
import { pool, withTx } from './db.js';

declare module 'fastify' {
  interface FastifyRequest {
    viewerRawBody: Buffer | null;
  }
}

const subscriptionInput = z.object({
  deliveryMode: z.enum(['at_most_once', 'at_least_once', 'exactly_once']),
  rules: z.object({
    type: z.string().min(1).optional(),
    source: z.string().min(1).optional(),
    payload: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])).optional(),
  }),
  failFirst: z.number().int().min(0).max(10).default(0),
});
const notificationInput = z.object({
  deliveryId: z.string().uuid(),
  eventId: z.string().uuid(),
  type: z.string(),
  source: z.string(),
  payload: z.record(z.string(), z.unknown()),
});

async function sessionId(request: FastifyRequest): Promise<string> {
  const parsed = z.string().uuid().safeParse(request.headers['x-test-viewer-session']);
  if (!parsed.success || !(await pool.query('SELECT id FROM test_viewer_sessions WHERE id=$1', [parsed.data])).rowCount) {
    throw Object.assign(new Error('A valid test viewer session is required'), { statusCode: 401 });
  }
  return parsed.data;
}

export async function registerTestViewer(app: FastifyInstance, callbackOrigin?: string) {
  const callback = new URL(callbackOrigin ?? 'http://127.0.0.1:3000');
  if (callback.protocol !== 'http:' || callback.hostname !== '127.0.0.1' || callback.pathname !== '/') {
    throw new Error('Test viewer callback must use the loopback HTTP origin');
  }
  await app.register(async viewer => {
    viewer.decorateRequest('viewerRawBody', null);
    viewer.removeContentTypeParser('application/json');
    viewer.addContentTypeParser('application/json', { parseAs: 'buffer' }, (request, body, done) => {
      request.viewerRawBody = Buffer.isBuffer(body) ? body : Buffer.from(body);
      try { done(null, JSON.parse(body.toString())); }
      catch { done(Object.assign(new Error('Invalid JSON'), { statusCode: 400 })); }
    });

    viewer.get('/', async (_, reply) => reply.type('text/html').send(
      await readFile(new URL('../public/test-viewer.html', import.meta.url), 'utf8'),
    ));
    viewer.get('/app.js', async (_, reply) => reply.type('application/javascript').send(
      await readFile(new URL('../public/test-viewer.js', import.meta.url), 'utf8'),
    ));
    viewer.post('/sessions', async (_, reply) => {
      const id = randomUUID();
      await pool.query('INSERT INTO test_viewer_sessions(id) VALUES($1)', [id]);
      return reply.code(201).send({ id });
    });
    viewer.post('/subscriptions', async (request, reply) => {
      const session = await sessionId(request);
      const parsed = subscriptionInput.safeParse(request.body);
      if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });
      const id = randomUUID();
      const secret = randomBytes(32).toString('hex');
      const token = randomBytes(24).toString('hex');
      const input = parsed.data;
      // Only this fixed test receiver bypasses public webhook URL registration.
      const address = viewer.server.address();
      const origin = callbackOrigin ?? `http://127.0.0.1:${address && typeof address !== 'string' ? address.port : 3000}`;
      const webhook = new URL(`/test-viewer/webhooks/${id}/${token}`, origin).href;
      await withTx(async tx => {
        await tx.query('INSERT INTO subscriptions(id,webhook_url,secret,delivery_mode,rules) VALUES($1,$2,$3,$4,$5)',
          [id, webhook, secret, input.deliveryMode, input.rules]);
        await tx.query('INSERT INTO test_viewer_subscriptions(subscription_id,session_id,callback_token,failures_remaining) VALUES($1,$2,$3,$4)',
          [id, session, token, input.failFirst]);
      });
      return reply.code(201).send({ id, ...input });
    });
    viewer.delete<{ Params: { id: string } }>('/subscriptions/:id', async (request, reply) => {
      const session = await sessionId(request);
      if (!z.string().uuid().safeParse(request.params.id).success) return reply.code(400).send({ error: 'Invalid subscription ID' });
      const result = await pool.query(`UPDATE subscriptions SET active=false WHERE id=$1 AND id IN
        (SELECT subscription_id FROM test_viewer_subscriptions WHERE session_id=$2)`, [request.params.id, session]);
      return result.rowCount ? { deleted: true } : reply.code(404).send({ error: 'Subscription not found in this session' });
    });
    viewer.get('/state', async request => {
      const session = await sessionId(request);
      const subscriptions = await pool.query(`SELECT s.id,s.delivery_mode,s.rules,s.active,v.failures_remaining,v.received_attempts
        FROM subscriptions s JOIN test_viewer_subscriptions v ON v.subscription_id=s.id
        WHERE v.session_id=$1 ORDER BY s.created_at DESC LIMIT 100`, [session]);
      const notifications = await pool.query(`SELECT r.* FROM test_viewer_receipts r
        JOIN test_viewer_subscriptions v ON v.subscription_id=r.subscription_id
        WHERE v.session_id=$1 ORDER BY r.last_received_at DESC LIMIT 100`, [session]);
      const deliveries = await pool.query(`SELECT d.id,d.event_id,d.subscription_id,d.status,d.attempt_count,d.final_reason
        FROM deliveries d JOIN test_viewer_subscriptions v ON v.subscription_id=d.subscription_id
        WHERE v.session_id=$1 ORDER BY d.created_at DESC LIMIT 100`, [session]);
      return { subscriptions: subscriptions.rows, notifications: notifications.rows, deliveries: deliveries.rows };
    });
    viewer.get<{ Params: { id: string } }>('/deliveries/:id/attempts', async (request, reply) => {
      const session = await sessionId(request);
      if (!z.string().uuid().safeParse(request.params.id).success) return reply.code(400).send({ error: 'Invalid delivery ID' });
      const attempts = await pool.query(`SELECT a.* FROM delivery_attempts a JOIN deliveries d ON d.id=a.delivery_id
        JOIN test_viewer_subscriptions v ON v.subscription_id=d.subscription_id
        WHERE a.delivery_id=$1 AND v.session_id=$2 ORDER BY a.attempt_no`, [request.params.id, session]);
      return attempts.rows;
    });
    viewer.post<{ Params: { id: string; token: string } }>('/webhooks/:id/:token', async (request, reply) => {
      if (!z.string().uuid().safeParse(request.params.id).success) return reply.code(404).send({ error: 'Unknown receiver' });
      const row = (await pool.query(`SELECT s.secret FROM subscriptions s JOIN test_viewer_subscriptions v
        ON v.subscription_id=s.id WHERE s.id=$1 AND v.callback_token=$2`, [request.params.id, request.params.token])).rows[0];
      if (!row) return reply.code(404).send({ error: 'Unknown receiver' });
      const signature = request.headers['x-messageq-signature'];
      if (typeof signature !== 'string' || !/^[0-9a-f]{64}$/i.test(signature) || !request.viewerRawBody) {
        return reply.code(401).send({ error: 'Invalid signature' });
      }
      const expected = createHmac('sha256', row.secret).update(request.viewerRawBody).digest();
      if (!timingSafeEqual(expected, Buffer.from(signature, 'hex'))) return reply.code(401).send({ error: 'Invalid signature' });
      const parsed = notificationInput.safeParse(request.body);
      if (!parsed.success) return reply.code(400).send({ error: parsed.error.issues });
      const body = parsed.data;
      const result = await withTx(async tx => {
        const delivery = await tx.query('SELECT id FROM deliveries WHERE id=$1 AND event_id=$2 AND subscription_id=$3',
          [body.deliveryId, body.eventId, request.params.id]);
        if (!delivery.rowCount) return 404;
        const receiver = (await tx.query('SELECT failures_remaining FROM test_viewer_subscriptions WHERE subscription_id=$1 FOR UPDATE',
          [request.params.id])).rows[0];
        await tx.query('UPDATE test_viewer_subscriptions SET received_attempts=received_attempts+1 WHERE subscription_id=$1', [request.params.id]);
        if (receiver.failures_remaining > 0) {
          await tx.query('UPDATE test_viewer_subscriptions SET failures_remaining=failures_remaining-1 WHERE subscription_id=$1', [request.params.id]);
          return 503;
        }
        // The receipt is the demo business effect; its unique ID and update commit together.
        await tx.query(`INSERT INTO test_viewer_receipts(delivery_id,subscription_id,event_id,notification)
          VALUES($1,$2,$3,$4) ON CONFLICT(delivery_id) DO UPDATE
          SET receive_count=test_viewer_receipts.receive_count+1,last_received_at=now()`,
          [body.deliveryId, request.params.id, body.eventId, body]);
        return 204;
      });
      return reply.code(result).send(result === 204 ? undefined : { error: result === 503 ? 'Simulated receiver failure' : 'Unknown delivery' });
    });
  }, { prefix: '/test-viewer' });
}
