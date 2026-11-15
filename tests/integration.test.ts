import { beforeAll, afterAll, afterEach, describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { buildApp } from '../src/app.js';
import { migrate, pool, fanoutOnce } from '../src/db.js';
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
  headers: { 'idempotency-key': key, 'x-producer-id': producer }, payload,
});
beforeAll(async () => {
  if (!process.env.DATABASE_URL) throw new Error('Integration tests require DATABASE_URL pointing to an isolated PostgreSQL database');
  await migrate();
  const unfinished = await pool.query("SELECT count(*)::integer AS count FROM fanout_jobs WHERE status <> 'done'");
  if (unfinished.rows[0].count !== 0) throw new Error('Queue tests require an isolated database with no unfinished jobs');
});
afterEach(async () => {
  for (const app of apps.splice(0)) await app.close();
  await pool.query('DELETE FROM events WHERE producer_id=$1', [producer]);
});
afterAll(async () => pool.end());
describe('HTTP validation integration', () => {
  it('requires an idempotency key and validates event shape', async () => {
    const app = buildApp();
    const response = await app.inject({ method: 'POST', url: '/events', payload: { type: 'x', source: 's', payload: {} } });
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
