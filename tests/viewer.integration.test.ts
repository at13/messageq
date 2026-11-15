import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import { randomUUID, createHmac } from 'node:crypto';
import { buildApp } from '../src/app.js';
import { migrate, pool, fanoutOnce, deliverOnce } from '../src/db.js';

const sessions: string[] = [];
const eventIds: string[] = [];
const app = buildApp({ testViewer: true });

beforeAll(async () => {
  if (!process.env.DATABASE_URL) throw new Error('Viewer integration tests require DATABASE_URL pointing to an isolated PostgreSQL database');
  await migrate();
  await app.listen({ port: 0, host: '127.0.0.1' });
});

afterAll(async () => {
  await app.close();
  for (const event of eventIds) await pool.query('DELETE FROM events WHERE id=$1', [event]);
  for (const session of sessions) {
    await pool.query(`DELETE FROM deliveries WHERE subscription_id IN
      (SELECT subscription_id FROM test_viewer_subscriptions WHERE session_id=$1)`, [session]);
    await pool.query(`DELETE FROM subscriptions WHERE id IN
      (SELECT subscription_id FROM test_viewer_subscriptions WHERE session_id=$1)`, [session]);
    await pool.query('DELETE FROM test_viewer_sessions WHERE id=$1', [session]);
  }
  await pool.end();
});

async function session() {
  const response = await app.inject({ method: 'POST', url: '/test-viewer/sessions' });
  expect(response.statusCode).toBe(201);
  const id = response.json().id as string;
  sessions.push(id);
  return { 'x-test-viewer-session': id };
}

async function setup(mode: string, failFirst = 0) {
  const headers = await session();
  const type = `viewer.test.${randomUUID()}`;
  const response = await app.inject({
    method: 'POST', url: '/test-viewer/subscriptions', headers,
    payload: { deliveryMode: mode, failFirst, rules: { type, payload: { 'customer.tier': 'gold' } } },
  });
  expect(response.statusCode).toBe(201);
  const subscriptionId = response.json().id as string;
  const event = await app.inject({
    method: 'POST', url: '/events',
    headers: { 'idempotency-key': randomUUID(), 'x-producer-id': headers['x-test-viewer-session'] },
    payload: { type, source: 'integration', payload: { customer: { tier: 'gold' }, message: '<script>not HTML</script>' } },
  });
  expect(event.statusCode).toBe(202);
  const eventId = event.json().id as string;
  eventIds.push(eventId);
  // Existing database jobs can coexist; process until our own event is routed.
  for (let i = 0; i < 100; i++) {
    await fanoutOnce();
    const delivery = await pool.query('SELECT * FROM deliveries WHERE event_id=$1', [eventId]);
    if (delivery.rowCount) return { headers, subscriptionId, eventId, delivery: delivery.rows[0] };
  }
  throw new Error('Test event was not routed within 100 fanout iterations');
}

async function sendOwnDelivery(id: string) {
  await pool.query('UPDATE deliveries SET next_attempt_at=now() WHERE id=$1', [id]);
  for (let i = 0; i < 100; i++) {
    await deliverOnce();
    const attempts = await pool.query('SELECT id FROM delivery_attempts WHERE delivery_id=$1', [id]);
    if (attempts.rowCount) return;
  }
  throw new Error('Test delivery was not attempted within 100 iterations');
}

describe('test viewer', () => {
  it('is disabled by default', async () => {
    const disabled = buildApp();
    expect((await disabled.inject('/test-viewer/')).statusCode).toBe(404);
    await disabled.close();
  });

  it('serves the UI and scopes management to a session without leaking callback secrets', async () => {
    const html = await app.inject('/test-viewer/');
    expect(html.statusCode).toBe(200);
    expect(html.body).toContain('MessageQ Test Viewer');
    expect((await app.inject('/test-viewer/state')).statusCode).toBe(401);
    const own = await setup('at_least_once');
    const foreign = await session();
    const state = (await app.inject({ url: '/test-viewer/state', headers: own.headers })).json();
    expect(state.subscriptions).toHaveLength(1);
    expect(state.subscriptions[0]).not.toHaveProperty('secret');
    expect(state.subscriptions[0]).not.toHaveProperty('callback_token');
    const foreignState = (await app.inject({ url: '/test-viewer/state', headers: foreign })).json();
    expect(foreignState.subscriptions).toHaveLength(0);
    expect(foreignState.deliveries).toHaveLength(0);
    expect((await app.inject({
      method: 'DELETE', url: `/test-viewer/subscriptions/${own.subscriptionId}`, headers: foreign,
    })).statusCode).toBe(404);
    expect((await app.inject({
      method: 'DELETE', url: `/test-viewer/subscriptions/${own.subscriptionId}`, headers: own.headers,
    })).statusCode).toBe(200);
  });

  it('rejects forged callbacks and persists a signed worker notification', async () => {
    const own = await setup('at_least_once');
    const callback = new URL(own.delivery.webhook_url).pathname;
    expect((await app.inject({
      method: 'POST', url: callback, payload: {},
    })).statusCode).toBe(401);
    await sendOwnDelivery(own.delivery.id);
    const state = (await app.inject({ url: '/test-viewer/state', headers: own.headers })).json();
    expect(state.notifications).toHaveLength(1);
    expect(state.notifications[0].notification.payload.message).toBe('<script>not HTML</script>');
    expect(state.deliveries[0].status).toBe('delivered');
  });

  it('records simulated failure and retries to success', async () => {
    const own = await setup('at_least_once', 1);
    await sendOwnDelivery(own.delivery.id);
    await pool.query('UPDATE deliveries SET next_attempt_at=now() WHERE id=$1', [own.delivery.id]);
    for (let i = 0; i < 100; i++) {
      await deliverOnce();
      if ((await pool.query('SELECT status FROM deliveries WHERE id=$1', [own.delivery.id])).rows[0].status === 'delivered') break;
    }
    const attempts = (await app.inject({
      url: `/test-viewer/deliveries/${own.delivery.id}/attempts`, headers: own.headers,
    })).json();
    expect(attempts.map((a: { response_code: number }) => a.response_code)).toEqual([503, 204]);
    const state = (await app.inject({ url: '/test-viewer/state', headers: own.headers })).json();
    expect(state.notifications).toHaveLength(1);
  });

  it('does not retry an at-most-once simulated failure', async () => {
    const own = await setup('at_most_once', 1);
    await sendOwnDelivery(own.delivery.id);
    await pool.query('UPDATE deliveries SET next_attempt_at=now(),locked_until=NULL WHERE id=$1', [own.delivery.id]);
    await deliverOnce();
    const state = (await app.inject({ url: '/test-viewer/state', headers: own.headers })).json();
    expect(state.deliveries[0].status).toBe('failed');
    expect(state.notifications).toHaveLength(0);
    expect(state.subscriptions[0].received_attempts).toBe(1);
  });

  it('deduplicates concurrent signed requests into one persistent demo business effect', async () => {
    const own = await setup('exactly_once');
    const body = JSON.stringify({
      deliveryId: own.delivery.id, eventId: own.eventId, type: 'test', source: 'integration', payload: { value: 1 },
    }, null, 2);
    const signature = createHmac('sha256', own.delivery.secret).update(body).digest('hex');
    const callback = new URL(own.delivery.webhook_url).pathname;
    const responses = await Promise.all(Array.from({ length: 3 }, () => app.inject({
      method: 'POST', url: callback, headers: { 'content-type': 'application/json', 'x-messageq-signature': signature }, payload: body,
    })));
    expect(responses.map(response => response.statusCode)).toEqual([204, 204, 204]);
    const state = (await app.inject({ url: '/test-viewer/state', headers: own.headers })).json();
    expect(state.notifications).toHaveLength(1);
    expect(state.notifications[0].receive_count).toBe(3);
  });
});
