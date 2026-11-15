import { Pool, type PoolClient } from 'pg';
import { readFile } from 'node:fs/promises';
import { randomUUID, createHmac } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import type { DeliveryMode, EventInput, FilterRules } from './types.js';
import { matches, retryDelay, shouldRetry } from './rules.js';

export const pool = new Pool({ connectionString: process.env.DATABASE_URL });
export function getEventQueueCapacity(value: string | number = process.env.EVENT_QUEUE_CAPACITY ?? 10000): number {
  const capacity = Number(value);
  if (!Number.isInteger(capacity) || capacity < 1 || capacity > 1_000_000) {
    throw new Error('EVENT_QUEUE_CAPACITY must be an integer between 1 and 1000000');
  }
  return capacity;
}

export class EventQueueFullError extends Error {
  readonly queuedEvents: number;
  readonly capacity: number;
  constructor(queuedEvents: number, capacity: number) {
    super('Event queue is full');
    this.name = 'EventQueueFullError';
    this.queuedEvents = queuedEvents;
    this.capacity = capacity;
  }
}
export async function migrate(): Promise<void> { await pool.query(await readFile(new URL('../schema.sql', import.meta.url), 'utf8')); }
export async function withTx<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try { await c.query('BEGIN'); const result = await fn(c); await c.query('COMMIT'); return result; }
  catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
}

export async function ingest(producerId: string, key: string, event: EventInput, capacity = getEventQueueCapacity()) {
  return withTx(async c => {
    // Coordinate admission and idempotency across replicas before checking capacity.
    await c.query("SELECT pg_advisory_xact_lock(hashtextextended('messageq:event-admission', 0))");
    const existing = await c.query('SELECT id, type, source, payload FROM events WHERE producer_id=$1 AND idempotency_key=$2', [producerId, key]);
    if (existing.rowCount) {
      const row = existing.rows[0];
      if (row.type !== event.type || row.source !== event.source || !isDeepStrictEqual(row.payload, event.payload)) throw Object.assign(new Error('idempotency key conflict'), { statusCode: 409 });
      return row;
    }
    const queued = (await c.query<{ count: number }>("SELECT count(*)::integer AS count FROM fanout_jobs WHERE status <> 'done'")).rows[0].count;
    if (queued >= capacity) throw new EventQueueFullError(queued, capacity);
    const id = randomUUID();
    await c.query('INSERT INTO events(id,type,source,payload,idempotency_key,producer_id) VALUES($1,$2,$3,$4,$5,$6)', [id, event.type, event.source, event.payload, key, producerId]);
    await c.query('INSERT INTO fanout_jobs(event_id) VALUES($1)', [id]);
    return { id, ...event };
  });
}

export async function fanoutOnce(): Promise<number> {
  return withTx(async c => {
    const job = await c.query(`UPDATE fanout_jobs SET status='processing', locked_until=now()+interval '30 seconds'
      WHERE event_id=(SELECT event_id FROM fanout_jobs WHERE status='pending' OR (status='processing' AND locked_until<now()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1)
      RETURNING event_id`);
    if (!job.rowCount) return 0;
    const event = (await c.query('SELECT * FROM events WHERE id=$1', [job.rows[0].event_id])).rows[0];
    const subs = await c.query('SELECT * FROM subscriptions WHERE active=true');
    for (const sub of subs.rows) if (matches(event, sub.rules as FilterRules)) {
      await c.query(`INSERT INTO deliveries(id,event_id,subscription_id,webhook_url,secret,delivery_mode)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(event_id,subscription_id) DO NOTHING`,
        [randomUUID(), event.id, sub.id, sub.webhook_url, sub.secret, sub.delivery_mode]);
    }
    await c.query("UPDATE fanout_jobs SET status='done',locked_until=NULL WHERE event_id=$1", [event.id]);
    return 1;
  });
}

async function postDelivery(delivery: any, event: any): Promise<{ status?: number; error?: string }> {
  const body = JSON.stringify({ deliveryId: delivery.id, eventId: event.id, type: event.type, source: event.source, payload: event.payload });
  const signature = createHmac('sha256', delivery.secret).update(body).digest('hex');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Number(process.env.WEBHOOK_TIMEOUT_MS ?? 5000));
  try {
    const response = await fetch(delivery.webhook_url, { method: 'POST', headers: { 'content-type': 'application/json', 'x-messageq-signature': signature, 'x-messageq-delivery-id': delivery.id }, body, signal: controller.signal });
    return { status: response.status };
  } catch (e) { return { error: e instanceof Error ? e.message : 'network error' }; }
  finally { clearTimeout(timer); }
}

export async function deliverOnce(): Promise<number> {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const result = await c.query(`UPDATE deliveries SET locked_until=now()+interval '30 seconds', attempt_count=attempt_count+1
      WHERE id=(SELECT id FROM deliveries WHERE status='pending' AND next_attempt_at<=now() AND (locked_until IS NULL OR locked_until<now()) ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1)
      RETURNING *`);
    if (!result.rowCount) { await c.query('COMMIT'); return 0; }
    const delivery = result.rows[0];
    if (delivery.delivery_mode === 'at_most_once') await c.query("UPDATE deliveries SET status='failed',final_reason='single attempt consumed' WHERE id=$1", [delivery.id]);
    await c.query('COMMIT');
    const event = (await pool.query('SELECT * FROM events WHERE id=$1', [delivery.event_id])).rows[0];
    const started = new Date();
    const outcome = await postDelivery(delivery, event);
    const retry = shouldRetry(outcome.status, outcome.error) && delivery.delivery_mode !== 'at_most_once';
    const status = outcome.status !== undefined && outcome.status >= 200 && outcome.status < 300;
    await withTx(async tx => {
      await tx.query('INSERT INTO delivery_attempts(delivery_id,attempt_no,started_at,completed_at,response_code,outcome,error) VALUES($1,$2,$3,now(),$4,$5,$6)', [delivery.id, delivery.attempt_count, started, outcome.status ?? null, status ? 'delivered' : (retry ? 'retry' : 'failed'), outcome.error ?? null]);
      if (status) await tx.query("UPDATE deliveries SET status='delivered',locked_until=NULL WHERE id=$1", [delivery.id]);
      else if (retry) await tx.query("UPDATE deliveries SET locked_until=NULL,next_attempt_at=now()+($2 * interval '1 millisecond') WHERE id=$1", [delivery.id, retryDelay(delivery.attempt_count)]);
      else await tx.query("UPDATE deliveries SET status='failed',locked_until=NULL,final_reason=$2 WHERE id=$1", [delivery.id, outcome.error ?? `HTTP ${outcome.status ?? 'unknown'}`]);
    });
    return 1;
  } catch (e) { try { await c.query('ROLLBACK'); } catch {} throw e; } finally { c.release(); }
}
