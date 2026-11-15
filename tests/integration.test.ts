import { beforeAll, afterAll, afterEach, describe, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { buildApp } from '../src/app.js';
import { migrate, pool, fanoutOnce, claimDelivery, completeDelivery } from '../src/db.js';
import * as urls from '../src/url.js';
const producer = `queue-test:${randomUUID()}`;
const event = { type: `queue.test.${randomUUID()}`, source: 'integration', payload: { customer: { tier: 'gold', region: 'us' }, count: 1 } };
const apps: ReturnType<typeof buildApp>[] = [];
const appWithCapacity = (capacity: number) => {
  const app = buildApp({ eventQueueCapacity: capacity });
  apps.push(app);
  return app;
};
const send = (app: ReturnType<typeof buildApp>, key: string, payload = event) => app.inject({
  method: 'POST', url: '/events',
  headers: { authorization: `Bearer ${process.env.API_TOKEN}`, 'idempotency-key': key, 'x-producer-id': producer }, payload,
});
beforeAll(async () => {
  if (!process.env.DATABASE_URL) throw new Error('Integration tests require DATABASE_URL pointing to an isolated PostgreSQL database');
  await migrate();
  const unfinished = await pool.query("SELECT count(*)::integer AS count FROM fanout_jobs WHERE status <> 'done'");
  if (unfinished.rows[0].count !== 0) throw new Error('Queue tests require an isolated database with no unfinished jobs');
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const app of apps.splice(0)) await app.close();
  await pool.query('DELETE FROM events WHERE producer_id=$1', [producer]);
});
afterAll(async () => pool.end());
describe('HTTP validation integration', () => {
  it('requires authentication without persisting unauthenticated events', async () => {
    const app = appWithCapacity(10);
    const result = await app.inject({ method: 'POST', url: '/events', payload: event });
    expect(result.statusCode).toBe(401);
    expect(result.headers['x-request-id']).toBeDefined();
    expect((await pool.query('SELECT id FROM events WHERE producer_id=$1', [producer])).rowCount).toBe(0);
    expect((await app.inject('/metrics')).statusCode).toBe(401);
  });
  it('persists arbitrary JSON, validates identifiers, and exposes health/metrics', async () => {
    const app = appWithCapacity(10);
    for (const payload of [null, 'hello', 12, false, [1, 2]]) {
      const result = await app.inject({
        method: 'POST', url: '/events', headers: { authorization: `Bearer ${process.env.API_TOKEN}`, 'idempotency-key': randomUUID(), 'x-producer-id': producer },
        payload: { ...event, payload },
      });
      expect(result.statusCode).toBe(202);
      expect((await pool.query('SELECT payload FROM events WHERE id=$1', [result.json().id])).rows[0].payload).toEqual(payload);
    }
    expect((await app.inject('/health/ready')).statusCode).toBe(200);
    expect((await app.inject({ method: 'HEAD', url: '/health/ready' })).statusCode).toBe(200);
    const metrics = await app.inject({ url: '/metrics', headers: { authorization: `Bearer ${process.env.API_TOKEN}` } });
    expect(metrics.statusCode).toBe(200);
    expect(metrics.body).toContain('messageq_backlog');
    expect((await app.inject({ url: '/events/not-a-uuid/deliveries', headers: { authorization: `Bearer ${process.env.API_TOKEN}` } })).statusCode).toBe(400);
  });
  it.each(['ECONNREFUSED', '53100', '40001'])('sanitizes database unavailability (%s) and returns retryable status', async code => {
    const app = appWithCapacity(10);
    vi.spyOn(pool, 'query').mockRejectedValueOnce(Object.assign(new Error('connection secret-value'), { code }));
    const response = await app.inject({ url: '/subscriptions', headers: { authorization: `Bearer ${process.env.API_TOKEN}` } });
    expect(response.statusCode).toBe(503);
    expect(response.headers['retry-after']).toBe('5');
    expect(response.body).not.toContain('secret-value');
  });
  it('rejects malformed JSON, unsupported media types, oversized requests and invalid pagination', async () => {
    const app = appWithCapacity(10);
    const headers = { authorization: `Bearer ${process.env.API_TOKEN}`, 'idempotency-key': randomUUID(), 'x-producer-id': producer };
    const malformed = await app.inject({ method: 'POST', url: '/events', headers: { ...headers, 'content-type': 'application/json' }, payload: '{' });
    expect(malformed.statusCode).toBe(400);
    const unsupported = await app.inject({ method: 'POST', url: '/events', headers: { ...headers, 'content-type': 'application/xml' }, payload: '<event />' });
    expect(unsupported.statusCode).toBe(415);
    const oversized = await app.inject({ method: 'POST', url: '/events', headers, payload: { ...event, payload: 'x'.repeat(1024 * 1024) } });
    expect(oversized.statusCode).toBe(413);
    expect((await app.inject({ url: '/subscriptions?limit=101', headers })).statusCode).toBe(400);
    expect((await pool.query('SELECT id FROM events WHERE producer_id=$1', [producer])).rowCount).toBe(0);
  });
  it('supports ordinary subscription CRUD without restart and rejects unsupported guarantees', async () => {
    const app = appWithCapacity(10);
    const headers = { authorization: `Bearer ${process.env.API_TOKEN}` };
    const body = { webhookUrl: 'https://subscriber.example/hook', secret: 'test-signing-secret-only', rules: { type: `crud.${randomUUID()}` }, deliveryMode: 'exactly_once' };
    expect((await app.inject({ method: 'POST', url: '/subscriptions', headers, payload: body })).statusCode).toBe(400);
    vi.spyOn(urls, 'isWebhookSafe').mockResolvedValue(true);
    const created = await app.inject({ method: 'POST', url: '/subscriptions', headers, payload: { ...body, deliveryMode: 'at_least_once' } });
    expect(created.statusCode).toBe(201);
    expect(created.json()).not.toHaveProperty('secret');
    const id = created.json().id;
    try {
      const listed = await app.inject({ url: '/subscriptions', headers });
      expect(listed.json().some((s: { id: string }) => s.id === id)).toBe(true);
      expect((await app.inject({ method: 'DELETE', url: `/subscriptions/${id}`, headers })).statusCode).toBe(200);
      expect((await pool.query('SELECT active FROM subscriptions WHERE id=$1', [id])).rows[0].active).toBe(false);
    } finally { await pool.query('DELETE FROM subscriptions WHERE id=$1', [id]); }
  });
  it('exposes delivered audit state, attempt timestamps/codes and pagination without signing secrets', async () => {
    const app = appWithCapacity(10);
    const headers = { authorization: `Bearer ${process.env.API_TOKEN}` };
    const secret = 'audit-test-signing-secret';
    vi.spyOn(urls, 'isWebhookSafe').mockResolvedValue(true);
    const created = await app.inject({
      method: 'POST', url: '/subscriptions', headers,
      payload: { webhookUrl: 'https://subscriber.example/audit', secret, rules: { type: event.type } },
    });
    expect(created.statusCode).toBe(201);
    const subscriptionId = created.json().id;
    try {
      const accepted = await send(app, randomUUID());
      expect(accepted.statusCode).toBe(202);
      expect(await fanoutOnce()).toBe(1);
      const claim = await claimDelivery();
      if (!claim) throw new Error('Expected a claimed test delivery');
      expect(await completeDelivery(claim, { status: 204 })).toBe(true);
      const history = await app.inject({ url: `/events/${accepted.json().id}/deliveries?limit=1`, headers });
      expect(history.statusCode).toBe(200);
      const delivery = history.json()[0];
      expect(delivery.status).toBe('delivered');
      expect(delivery.attempts).toHaveLength(1);
      expect(delivery.attempts[0].response_code).toBe(204);
      expect(Number.isFinite(Date.parse(delivery.attempts[0].started_at))).toBe(true);
      expect(Number.isFinite(Date.parse(delivery.attempts[0].completed_at))).toBe(true);
      for (const url of [`/deliveries/${delivery.id}`, `/subscriptions/${subscriptionId}/deliveries`, `/deliveries/${delivery.id}/attempts?limit=1`]) {
        const response = await app.inject({ url, headers });
        expect(response.statusCode).toBe(200);
        expect(response.body).not.toContain(secret);
      }
      expect(history.body).not.toContain(secret);
      const emptyPage = await app.inject({ url: `/deliveries/${delivery.id}/attempts?offset=1`, headers });
      expect(emptyPage.json()).toEqual([]);
    } finally {
      await pool.query('DELETE FROM events WHERE producer_id=$1', [producer]);
      await pool.query('DELETE FROM subscriptions WHERE id=$1', [subscriptionId]);
    }
  });
  it('requires an idempotency key and validates event shape', async () => {
    const app = buildApp();
    const response = await app.inject({ method: 'POST', url: '/events', headers: { authorization: `Bearer ${process.env.API_TOKEN}` }, payload: { type: 'x', source: 's', payload: {} } });
    expect(response.statusCode).toBe(400);
    await app.close();
  });

  describe('bounded durable event queue', () => {
    it('rejects new events with 503 and Retry-After without storing rejected work', async () => {
      const app = appWithCapacity(1);
      expect((await send(app, 'first')).statusCode).toBe(202);
      const rejected = await send(app, 'second');
      expect(rejected.statusCode).toBe(503);
      expect(rejected.headers['retry-after']).toBe('5');
      expect(rejected.json()).toEqual({ code: 'EVENT_QUEUE_FULL', error: 'Event queue is full', capacity: 1, queuedEvents: 1 });
      expect((await pool.query('SELECT id FROM events WHERE producer_id=$1', [producer])).rowCount).toBe(1);
      expect((await pool.query('SELECT j.event_id FROM fanout_jobs j JOIN events e ON e.id=j.event_id WHERE e.producer_id=$1', [producer])).rowCount).toBe(1);
    });

    it('returns the accepted event on retry while full, ignoring object key order', async () => {
      const app = appWithCapacity(1);
      const original = await send(app, 'same');
      const repeated = await send(app, 'same', { ...event, payload: { count: 1, customer: { region: 'us', tier: 'gold' } } });
      expect(repeated.statusCode).toBe(202);
      expect(repeated.json().id).toBe(original.json().id);
      expect((await send(app, 'same', { ...event, source: 'different' })).statusCode).toBe(409);
    });

    it('does not exceed capacity when separate API instances race', async () => {
      const first = appWithCapacity(2), second = appWithCapacity(2);
      const responses = await Promise.all(Array.from({ length: 8 }, (_, i) => send(i % 2 ? first : second, `burst-${i}`)));
      expect(responses.filter(response => response.statusCode === 202)).toHaveLength(2);
      expect(responses.filter(response => response.statusCode === 503)).toHaveLength(6);
      expect((await pool.query("SELECT count(*)::integer AS count FROM fanout_jobs WHERE status <> 'done'")).rows[0].count).toBe(2);
    });

    it('accepts concurrent retries of one key without duplicates or queue errors', async () => {
      const first = appWithCapacity(1), second = appWithCapacity(1);
      const responses = await Promise.all(Array.from({ length: 6 }, (_, i) => send(i % 2 ? first : second, 'concurrent-same')));
      expect(responses.every(response => response.statusCode === 202)).toBe(true);
      expect(new Set(responses.map(response => response.json().id)).size).toBe(1);
      expect((await pool.query('SELECT id FROM events WHERE producer_id=$1', [producer])).rowCount).toBe(1);
    });

    it('releases capacity only after routing completes, retaining accepted events', async () => {
      const app = appWithCapacity(1);
      const accepted = await send(app, 'before');
      await pool.query("UPDATE fanout_jobs SET status='processing', locked_until=now()-interval '1 second' WHERE event_id=$1", [accepted.json().id]);
      expect((await send(app, 'after')).statusCode).toBe(503);
      expect(await fanoutOnce()).toBe(1);
      expect((await send(app, 'after')).statusCode).toBe(202);
      expect((await pool.query('SELECT id FROM events WHERE producer_id=$1', [producer])).rowCount).toBe(2);
    });
  });
});
