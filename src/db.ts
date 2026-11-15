import { Pool, type PoolClient } from 'pg';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import type { DeliveryMode, EventInput, FilterRules } from './types.js';
import { matches, retryDelay, shouldRetry } from './rules.js';
import { sendWebhook } from './webhook.js';

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 5000,
  query_timeout: 10_000,
  statement_timeout: 10_000,
  idle_in_transaction_session_timeout: 10_000,
});
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
    await c.query('INSERT INTO events(id,type,source,payload,idempotency_key,producer_id) VALUES($1,$2,$3,$4::jsonb,$5,$6)', [id, event.type, event.source, JSON.stringify(event.payload), key, producerId]);
    await c.query('INSERT INTO fanout_jobs(event_id) VALUES($1)', [id]);
    return { id, ...event };
  });
}

export async function fanoutOnce(): Promise<number> {
  // One atomic batch using the active subscriptions observed in this transaction.
  // There are no durable subscription versions or resumable fanout cursors.
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

export type AttemptOutcome = 'delivered' | 'retry' | 'failed' | 'unknown';
export type WebhookResult = Awaited<ReturnType<typeof sendWebhook>>;
export type DeliveryClaim = {
  id: string;
  event_id: string;
  webhook_url: string;
  secret: string;
  delivery_mode: DeliveryMode;
  attempt_count: number;
  claim_token: string;
  allowLoopback: boolean;
  event: EventInput;
};
export type AttemptObservation = {
  deliveryId: string;
  attempt: number;
  outcome: AttemptOutcome;
  status?: number;
  durationMs: number;
  applied: boolean;
};
export type DeliveryOptions = {
  timeoutMs?: number;
  leaseMs?: number;
  onAttempt?: (attempt: AttemptObservation) => void;
};

function positiveMilliseconds(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
  return value;
}

async function expireClaim(c: PoolClient, delivery: {
  id: string; attempt_count: number; claim_token: string | null; delivery_mode: DeliveryMode;
}): Promise<void> {
  // Old installations could have committed a claim without an audit row.
  if (delivery.attempt_count > 0) {
    await c.query(`INSERT INTO delivery_attempts(delivery_id,attempt_no,claim_token,started_at,completed_at,outcome,error)
      VALUES($1,$2,$3,now(),now(),'unknown','lease expired; remote outcome unknown')
      ON CONFLICT(delivery_id,attempt_no) DO UPDATE
      SET completed_at=now(),outcome='unknown',error=EXCLUDED.error
      WHERE delivery_attempts.completed_at IS NULL`,
      [delivery.id, delivery.attempt_count, delivery.claim_token]);
  }
  await c.query(`UPDATE deliveries SET status=$2,locked_until=NULL,claim_token=NULL,
    next_attempt_at=now(),final_reason=$3 WHERE id=$1`, [
    delivery.id, delivery.delivery_mode === 'at_most_once' ? 'unknown' : 'pending',
    delivery.delivery_mode === 'at_most_once' ? 'single authorization consumed; remote outcome unknown' : null,
  ]);
}

/** Reclaims a bounded batch, never restoring an at-most-once authorization. */
export async function reconcileExpiredLeases(limit = 100): Promise<number> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) throw new Error('lease reconciliation limit must be between 1 and 1000');
  return withTx(async c => {
    const expired = await c.query(`SELECT id,attempt_count,claim_token,delivery_mode FROM deliveries
      WHERE (locked_until<=now() AND status IN ('in_flight','pending'))
        OR (delivery_mode='at_most_once' AND attempt_count>0 AND status='pending' AND locked_until IS NULL)
        OR (delivery_mode='at_most_once' AND final_reason='single attempt consumed' AND status='failed' AND locked_until<=now())
      ORDER BY locked_until NULLS FIRST,created_at FOR UPDATE SKIP LOCKED LIMIT $1`, [limit]);
    for (const delivery of expired.rows) await expireClaim(c, delivery);
    return expired.rowCount ?? 0;
  });
}

/** The authorization and its audit row commit together, before any network I/O. */
export async function claimDelivery(leaseMs = 30_000): Promise<DeliveryClaim | undefined> {
  positiveMilliseconds(leaseMs, 'leaseMs');
  await reconcileExpiredLeases();
  return withTx(async c => {
    const token = randomUUID();
    const result = await c.query<Omit<DeliveryClaim, 'event' | 'allowLoopback'> & { allow_loopback: boolean }>(`UPDATE deliveries SET status='in_flight',
      locked_until=now()+($1 * interval '1 millisecond'),claim_token=$2,attempt_count=attempt_count+1
      WHERE id=(SELECT id FROM deliveries WHERE status='pending' AND next_attempt_at<=now()
        AND locked_until IS NULL AND (delivery_mode<>'at_most_once' OR attempt_count=0)
        ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1)
      RETURNING id,event_id,webhook_url,secret,delivery_mode,attempt_count,claim_token,
        EXISTS(SELECT 1 FROM test_viewer_subscriptions WHERE subscription_id=deliveries.subscription_id) AS allow_loopback`, [leaseMs, token]);
    if (!result.rowCount) return undefined;
    const delivery = result.rows[0];
    await c.query(`INSERT INTO delivery_attempts(delivery_id,attempt_no,claim_token,started_at,outcome)
      VALUES($1,$2,$3,now(),'authorized')`, [delivery.id, delivery.attempt_count, token]);
    const event = (await c.query<EventInput>('SELECT type,source,payload FROM events WHERE id=$1', [delivery.event_id])).rows[0];
    const { allow_loopback, ...fields } = delivery;
    return { ...fields, event, allowLoopback: allow_loopback };
  });
}

function classifyResult(result: WebhookResult, mode: DeliveryMode): { outcome: AttemptOutcome; retry: boolean } {
  if (result.status !== undefined && result.status >= 200 && result.status < 300 && !result.error) {
    return { outcome: 'delivered', retry: false };
  }
  const retry = mode !== 'at_most_once' && shouldRetry(result.status, result.error);
  return { outcome: result.error || result.status === undefined ? 'unknown' : retry ? 'retry' : 'failed', retry };
}

/** Fences late responses: only the current, unexpired claim may change state. */
export async function completeDelivery(claim: DeliveryClaim, result: WebhookResult): Promise<boolean> {
  return withTx(async c => {
    const current = await c.query(`SELECT id,attempt_count,claim_token,delivery_mode,status
      FROM deliveries WHERE id=$1 FOR UPDATE`, [claim.id]);
    const delivery = current.rows[0];
    if (!delivery || delivery.claim_token !== claim.claim_token || delivery.status !== 'in_flight') return false;
    const { outcome, retry } = classifyResult(result, delivery.delivery_mode);
    const delay = retry ? Math.max(retryDelay(delivery.attempt_count), result.retryAfterMs ?? 0) : 0;
    const updated = await c.query(`UPDATE deliveries SET status=$2,locked_until=NULL,claim_token=NULL,
      next_attempt_at=now()+($3 * interval '1 millisecond'),final_reason=$4
      WHERE id=$1 AND locked_until>clock_timestamp() RETURNING id`,
      [claim.id, retry ? 'pending' : outcome, delay,
        outcome === 'delivered' || retry ? null : result.error ?? `HTTP ${result.status ?? 'unknown'}`]);
    if (!updated.rowCount) {
      await expireClaim(c, delivery);
      return false;
    }
    await c.query(`UPDATE delivery_attempts SET completed_at=now(),response_code=$3,outcome=$4,error=$5
      WHERE delivery_id=$1 AND claim_token=$2 AND completed_at IS NULL`,
      [claim.id, claim.claim_token, result.status ?? null, outcome, result.error ?? null]);
    return true;
  });
}

export async function deliverOnce(options: DeliveryOptions = {}): Promise<number> {
  const timeoutMs = positiveMilliseconds(options.timeoutMs ?? Number(process.env.WEBHOOK_TIMEOUT_MS ?? 5000), 'timeoutMs');
  const leaseMs = positiveMilliseconds(options.leaseMs ?? Math.max(30_000, timeoutMs + 1000), 'leaseMs');
  if (leaseMs <= timeoutMs) throw new Error('leaseMs must exceed timeoutMs');
  const claim = await claimDelivery(leaseMs);
  if (!claim) return 0;
  // claimDelivery has released its client, including the event lookup, before send.
  const started = performance.now();
  let result: WebhookResult;
  try {
    result = await sendWebhook({
      url: claim.webhook_url, secret: claim.secret, deliveryId: claim.id,
      eventId: claim.event_id, allowLoopback: claim.allowLoopback, ...claim.event,
    }, timeoutMs);
  } catch {
    result = { error: 'webhook transport failed' };
  }
  const durationMs = performance.now() - started;
  let applied = false;
  try {
    applied = await completeDelivery(claim, result);
  } finally {
    // Observability must not decide whether a durable attempt is retried.
    try {
      options.onAttempt?.({
        deliveryId: claim.id, attempt: claim.attempt_count,
        outcome: applied ? classifyResult(result, claim.delivery_mode).outcome : 'unknown',
        status: result.status, durationMs, applied,
      });
    } catch {}
  }
  return 1;
}
