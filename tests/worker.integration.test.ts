import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { createHmac, randomBytes, randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  claimDelivery, completeDelivery, deliverOnce, fanoutOnce, ingest, migrate, pool,
  reconcileExpiredLeases, type DeliveryClaim,
} from '../src/db.js';
import { startWorkers, type Workers } from '../src/worker.js';
import type { DeliveryMode, JSONValue } from '../src/types.js';

const producer = `worker-test:${randomUUID()}`;
const subscriptionIds: string[] = [];
const workers: Workers[] = [];
const sessionIds: string[] = [];
const releaseResponses: (() => void)[] = [];
const pendingSends: Promise<unknown>[] = [];
const fixtureErrors: unknown[] = [];
const faultTrigger = 'worker_test_fault';
const faultFunction = 'worker_test_fault_fn';
type FixtureHandler = (request: IncomingMessage, response: ServerResponse, body: string) => Promise<void>;
let fixtureHandler: FixtureHandler = async (_, response) => { response.writeHead(204).end(); };
const fixture = createServer((request, response) => {
  void (async () => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    await fixtureHandler(request, response, Buffer.concat(chunks).toString());
  })().catch(error => {
    fixtureErrors.push(error);
    response.writeHead(500).end();
  });
});
let fixtureOrigin: string;

beforeAll(async () => {
  if (!process.env.DATABASE_URL) throw new Error('Worker tests require DATABASE_URL pointing to an isolated PostgreSQL database');
  await migrate();
  const unfinished = await pool.query(`SELECT
    (SELECT count(*) FROM fanout_jobs WHERE status<>'done') +
    (SELECT count(*) FROM deliveries WHERE status IN ('pending','in_flight')) AS count`);
  if (Number(unfinished.rows[0].count) !== 0) throw new Error('Worker tests require an isolated database without unfinished work');
  await new Promise<void>(resolve => fixture.listen(0, '127.0.0.1', resolve));
  fixtureOrigin = `http://127.0.0.1:${(fixture.address() as AddressInfo).port}`;
});

async function removeFault() {
  for (const table of ['fanout_jobs', 'deliveries', 'delivery_attempts']) {
    await pool.query(`DROP TRIGGER IF EXISTS ${faultTrigger} ON ${table}`);
  }
  await pool.query(`DROP FUNCTION IF EXISTS ${faultFunction}()`);
}

afterEach(async () => {
  for (const release of releaseResponses.splice(0)) release();
  await Promise.all(workers.splice(0).map(worker => worker.stop()));
  await Promise.allSettled(pendingSends.splice(0));
  fixture.closeAllConnections();
  fixtureHandler = async (_, response) => { response.writeHead(204).end(); };
  await removeFault();
  await pool.query('DELETE FROM events WHERE producer_id=$1', [producer]);
  await pool.query('DELETE FROM subscriptions WHERE id=ANY($1::uuid[])', [subscriptionIds.splice(0)]);
  await pool.query('DELETE FROM test_viewer_sessions WHERE id=ANY($1::uuid[])', [sessionIds.splice(0)]);
  expect(fixtureErrors.splice(0)).toEqual([]);
});
afterAll(async () => {
  fixture.closeAllConnections();
  await new Promise<void>(resolve => fixture.close(() => resolve()));
  await pool.end();
});

async function faultOn(table: 'fanout_jobs' | 'deliveries' | 'delivery_attempts', condition = 'true', guard = 'true') {
  await pool.query(`CREATE FUNCTION ${faultFunction}() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF ${guard} THEN RAISE EXCEPTION 'worker test injected rollback'; END IF;
      RETURN NEW;
    END $$`);
  await pool.query(`CREATE TRIGGER ${faultTrigger} BEFORE INSERT ON ${table}
    FOR EACH ROW WHEN (${condition}) EXECUTE FUNCTION ${faultFunction}()`);
}

async function subscription(type: string, mode: DeliveryMode = 'at_least_once', url = 'https://example.com/webhook') {
  const id = randomUUID();
  subscriptionIds.push(id);
  await pool.query(`INSERT INTO subscriptions(id,webhook_url,secret,delivery_mode,rules)
    VALUES($1,$2,'worker-test-secret',$3,$4::jsonb)`, [id, url, mode, JSON.stringify({ type })]);
  return id;
}

async function queued(mode: DeliveryMode = 'at_least_once', payload: JSONValue = { value: 1 }, url?: string) {
  const type = `worker.test.${randomUUID()}`;
  const subscriptionId = await subscription(type, mode, url);
  const event = await ingest(producer, randomUUID(), { type, source: 'worker-integration', payload });
  expect(await fanoutOnce()).toBe(1);
  const delivery = (await pool.query('SELECT * FROM deliveries WHERE event_id=$1', [event.id])).rows[0];
  return { event, subscriptionId, delivery };
}

async function requireClaim(leaseMs = 30_000): Promise<DeliveryClaim> {
  const claim = await claimDelivery(leaseMs);
  expect(claim).toBeDefined();
  return claim!;
}

function signal() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

async function localQueued(mode: DeliveryMode = 'at_least_once', payload: JSONValue = { value: 1 }) {
  const own = await queued(mode, payload);
  const sessionId = randomUUID(), callbackToken = randomBytes(24).toString('hex');
  sessionIds.push(sessionId);
  const url = `${fixtureOrigin}/test-viewer/webhooks/${own.subscriptionId}/${callbackToken}`;
  await pool.query('INSERT INTO test_viewer_sessions(id) VALUES($1)', [sessionId]);
  await pool.query(`INSERT INTO test_viewer_subscriptions(subscription_id,session_id,callback_token)
    VALUES($1,$2,$3)`, [own.subscriptionId, sessionId, callbackToken]);
  await pool.query('UPDATE subscriptions SET webhook_url=$2 WHERE id=$1', [own.subscriptionId, url]);
  await pool.query('UPDATE deliveries SET webhook_url=$2 WHERE id=$1', [own.delivery.id, url]);
  return own;
}

async function expire(id: string) {
  await pool.query("UPDATE deliveries SET locked_until=now()-interval '1 second' WHERE id=$1", [id]);
}

async function history(id: string) {
  return (await pool.query('SELECT * FROM delivery_attempts WHERE delivery_id=$1 ORDER BY attempt_no', [id])).rows;
}

describe('durable worker authorization and recovery', () => {
  it('rolls back admission completely when durable fanout enqueue fails', async () => {
    await faultOn('fanout_jobs');
    const key = randomUUID(), event = { type: 'worker.rollback', source: 'worker-integration', payload: null };
    await expect(ingest(producer, key, event)).rejects.toThrow('worker test injected rollback');
    expect((await pool.query('SELECT id FROM events WHERE producer_id=$1 AND idempotency_key=$2', [producer, key])).rowCount).toBe(0);
    await removeFault();
    const accepted = await ingest(producer, key, event);
    expect((await pool.query('SELECT event_id FROM fanout_jobs WHERE event_id=$1', [accepted.id])).rowCount).toBe(1);
    expect((await ingest(producer, key, event)).id).toBe(accepted.id);
  });

  it('keeps fanout atomic after a partial insert failure, reclaims abandoned jobs and deduplicates rerouting', async () => {
    const type = `worker.fanout.${randomUUID()}`;
    await subscription(type);
    await subscription(type);
    const event = await ingest(producer, randomUUID(), { type, source: 'worker-integration', payload: ['value', null] });
    await faultOn('deliveries', 'true', 'EXISTS(SELECT 1 FROM deliveries WHERE event_id=NEW.event_id)');
    await expect(fanoutOnce()).rejects.toThrow('worker test injected rollback');
    expect((await pool.query('SELECT id FROM deliveries WHERE event_id=$1', [event.id])).rowCount).toBe(0);
    expect((await pool.query('SELECT status FROM fanout_jobs WHERE event_id=$1', [event.id])).rows[0].status).toBe('pending');
    await removeFault();
    await pool.query("UPDATE fanout_jobs SET status='processing',locked_until=now()-interval '1 second' WHERE event_id=$1", [event.id]);
    expect((await Promise.all(Array.from({ length: 4 }, () => fanoutOnce()))).reduce((sum, count) => sum + count, 0)).toBe(1);
    const original = (await pool.query('SELECT id FROM deliveries WHERE event_id=$1 ORDER BY id', [event.id])).rows;
    expect(original).toHaveLength(2);
    await pool.query("UPDATE fanout_jobs SET status='pending' WHERE event_id=$1", [event.id]);
    expect(await fanoutOnce()).toBe(1);
    expect((await pool.query('SELECT id FROM deliveries WHERE event_id=$1 ORDER BY id', [event.id])).rows).toEqual(original);
  });

  it('rolls back claim and attempt count if the pre-send audit insert fails', async () => {
    const { delivery } = await queued();
    await faultOn('delivery_attempts');
    await expect(claimDelivery()).rejects.toThrow('worker test injected rollback');
    const unchanged = (await pool.query('SELECT * FROM deliveries WHERE id=$1', [delivery.id])).rows[0];
    expect(unchanged).toMatchObject({ status: 'pending', attempt_count: 0, claim_token: null, locked_until: null });
    expect(await history(delivery.id)).toHaveLength(0);
    await removeFault();
    const claim = await requireClaim();
    expect(await history(delivery.id)).toMatchObject([{ attempt_no: 1, claim_token: claim.claim_token, outcome: 'authorized', completed_at: null }]);
  });

  it('never restores an at-most-once authorization across the pre-send crash gap', async () => {
    const { delivery } = await queued('at_most_once');
    await requireClaim();
    await expire(delivery.id);
    expect(await reconcileExpiredLeases()).toBe(1);
    expect(await claimDelivery()).toBeUndefined();
    expect((await pool.query('SELECT status,attempt_count FROM deliveries WHERE id=$1', [delivery.id])).rows[0]).toEqual({ status: 'unknown', attempt_count: 1 });
    const attempts = await history(delivery.id);
    expect(attempts).toMatchObject([{ attempt_no: 1, outcome: 'unknown', response_code: null }]);
    expect(attempts[0].completed_at).toBeInstanceOf(Date);
    // Even an accidentally reset pending status cannot grant a second authorization.
    await pool.query("UPDATE deliveries SET status='pending' WHERE id=$1", [delivery.id]);
    expect(await claimDelivery()).toBeUndefined();
    expect(await history(delivery.id)).toHaveLength(1);
  });

  it('reclaims retry-enabled leases with unknown history and fences late stale results after success', async () => {
    const { delivery } = await queued('exactly_once');
    const old = await requireClaim();
    await expire(delivery.id);
    const current = await requireClaim();
    expect(current.attempt_count).toBe(2);
    expect(current.claim_token).not.toBe(old.claim_token);
    expect(await completeDelivery(old, { status: 204 })).toBe(false);
    expect(await completeDelivery(current, { status: 204 })).toBe(true);
    expect(await completeDelivery(old, { status: 503 })).toBe(false);
    expect(await completeDelivery(current, { error: 'late duplicate' })).toBe(false);
    expect((await pool.query('SELECT status,attempt_count FROM deliveries WHERE id=$1', [delivery.id])).rows[0]).toEqual({ status: 'delivered', attempt_count: 2 });
    expect(await history(delivery.id)).toMatchObject([
      { attempt_no: 1, outcome: 'unknown', response_code: null },
      { attempt_no: 2, outcome: 'delivered', response_code: 204 },
    ]);
  });

  it('rejects a response whose own lease expired before completion, even before another worker reclaims it', async () => {
    const { delivery } = await queued();
    const claim = await requireClaim();
    await expire(delivery.id);
    expect(await completeDelivery(claim, { status: 204 })).toBe(false);
    expect(await history(delivery.id)).toMatchObject([{ outcome: 'unknown', response_code: null }]);
    expect((await pool.query('SELECT status FROM deliveries WHERE id=$1', [delivery.id])).rows[0].status).toBe('pending');
    expect((await requireClaim()).attempt_count).toBe(2);
  });

  it('grants only one concurrent authorization for a ready delivery', async () => {
    const { delivery } = await queued();
    const claims = (await Promise.all(Array.from({ length: 8 }, () => claimDelivery()))).filter(claim => claim !== undefined);
    expect(claims).toHaveLength(1);
    expect(await history(delivery.id)).toHaveLength(1);
    expect(await completeDelivery(claims[0]!, { status: 204 })).toBe(true);
  });

  it.each(([null, true, 42, 'text', [1, null, { nested: false }]] as JSONValue[]).map(payload => ({ payload })))('stores and routes JSON payload $payload without object-only assumptions', async ({ payload }) => {
    const { delivery } = await queued('at_least_once', payload);
    const claim = await requireClaim();
    expect(claim.id).toBe(delivery.id);
    expect(claim.event.payload).toEqual(payload);
  });

  it('retries transient responses with Retry-After, then retains permanent failures without further authorization', async () => {
    const { delivery } = await queued();
    const first = await requireClaim();
    expect(await completeDelivery(first, { status: 429, retryAfterMs: 120_000 })).toBe(true);
    const deferred = (await pool.query('SELECT status,next_attempt_at>now()+interval \'110 seconds\' AS delayed FROM deliveries WHERE id=$1', [delivery.id])).rows[0];
    expect(deferred).toEqual({ status: 'pending', delayed: true });
    expect(await claimDelivery()).toBeUndefined();
    await pool.query('UPDATE deliveries SET next_attempt_at=now() WHERE id=$1', [delivery.id]);
    expect(await completeDelivery(await requireClaim(), { status: 400 })).toBe(true);
    expect(await claimDelivery()).toBeUndefined();
    expect((await pool.query('SELECT status FROM deliveries WHERE id=$1', [delivery.id])).rows[0].status).toBe('failed');
    expect(await history(delivery.id)).toMatchObject([{ outcome: 'retry', response_code: 429 }, { outcome: 'failed', response_code: 400 }]);
  });

  it.each(['at_most_once', 'at_least_once'] as const)('records ambiguous transport outcomes separately for %s', async mode => {
    const { delivery } = await queued(mode);
    expect(await completeDelivery(await requireClaim(), { error: 'webhook timed out' })).toBe(true);
    expect(await history(delivery.id)).toMatchObject([{ outcome: 'unknown', response_code: null, error: 'webhook timed out' }]);
    expect((await pool.query('SELECT status FROM deliveries WHERE id=$1', [delivery.id])).rows[0].status).toBe(mode === 'at_most_once' ? 'unknown' : 'pending');
  });

  it('commits authorization before HTTP send and releases all claim connections while the receiver is blocked', async () => {
    const { delivery } = await localQueued('at_least_once', 42);
    const entered = signal(), release = signal();
    releaseResponses.push(release.resolve);
    fixtureHandler = async (request, response, body) => {
      expect(request.headers['x-messageq-signature']).toBe(createHmac('sha256', 'worker-test-secret').update(body).digest('hex'));
      expect(JSON.parse(body)).toMatchObject({ deliveryId: delivery.id, payload: 42 });
      expect(await history(delivery.id)).toMatchObject([{ attempt_no: 1, outcome: 'authorized', completed_at: null }]);
      expect((await pool.query('SELECT status FROM deliveries WHERE id=$1', [delivery.id])).rows[0].status).toBe('in_flight');
      entered.resolve();
      await release.promise;
      response.writeHead(204).end();
    };
    const observations: unknown[] = [];
    const sending = deliverOnce({ timeoutMs: 2000, leaseMs: 5000, onAttempt: attempt => observations.push(attempt) });
    pendingSends.push(sending);
    await entered.promise;
    expect(pool.waitingCount).toBe(0);
    expect(pool.idleCount).toBe(pool.totalCount);
    release.resolve();
    expect(await sending).toBe(1);
    expect(await history(delivery.id)).toMatchObject([{ outcome: 'delivered', response_code: 204 }]);
    expect(observations).toMatchObject([{ deliveryId: delivery.id, outcome: 'delivered', status: 204, applied: true }]);
  });

  it('records a real HTTP timeout as unknown, then retries successfully without losing that audit history', async () => {
    const { delivery } = await localQueued();
    const release = signal();
    releaseResponses.push(release.resolve);
    fixtureHandler = async (_, response) => {
      await release.promise;
      response.writeHead(204).end();
    };
    expect(await deliverOnce({ timeoutMs: 50, leaseMs: 5000 })).toBe(1);
    expect(await history(delivery.id)).toMatchObject([{ outcome: 'unknown', response_code: null }]);
    expect((await pool.query('SELECT status FROM deliveries WHERE id=$1', [delivery.id])).rows[0].status).toBe('pending');
    release.resolve();
    fixtureHandler = async (_, response) => { response.writeHead(204).end(); };
    await pool.query('UPDATE deliveries SET next_attempt_at=now() WHERE id=$1', [delivery.id]);
    expect(await deliverOnce({ timeoutMs: 1000, leaseMs: 5000 })).toBe(1);
    expect(await history(delivery.id)).toMatchObject([{ outcome: 'unknown' }, { outcome: 'delivered', response_code: 204 }]);
  });

  it('keeps polling non-overlapping and stop awaits the active HTTP attempt without authorizing the next delivery', async () => {
    const first = await localQueued(), second = await localQueued();
    const entered = signal(), release = signal();
    releaseResponses.push(release.resolve);
    let active = 0, maximum = 0, received = 0;
    fixtureHandler = async (_, response) => {
      active++;
      received++;
      maximum = Math.max(maximum, active);
      entered.resolve();
      await release.promise;
      active--;
      response.writeHead(204).end();
    };
    const worker = startWorkers({ intervalMs: 5, batchSize: 10, timeoutMs: 2000, leaseMs: 5000 });
    workers.push(worker);
    await entered.promise;
    await new Promise(resolve => setTimeout(resolve, 40));
    expect(received).toBe(1);
    expect(maximum).toBe(1);
    let stopped = false;
    const stopping = worker.stop().then(() => { stopped = true; });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(stopped).toBe(false);
    release.resolve();
    await stopping;
    const statuses = (await pool.query('SELECT status FROM deliveries WHERE id=ANY($1::uuid[]) ORDER BY status', [[first.delivery.id, second.delivery.id]])).rows;
    expect(statuses).toEqual([{ status: 'delivered' }, { status: 'pending' }]);
    expect(received).toBe(1);
  });

  it('stops an idle worker promptly and idempotently without waiting for its next poll', async () => {
    const worker = startWorkers({ intervalMs: 60_000 });
    workers.push(worker);
    await new Promise(resolve => setTimeout(resolve, 25));
    await Promise.race([
      Promise.all([worker.stop(), worker.stop()]),
      new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('worker stop did not wake idle poll')), 1000);
        timer.unref();
      }),
    ]);
  });
});
