import type { PoolClient } from 'pg';
import { randomUUID } from 'node:crypto';
import type { DeliveryMode, EventInput, FilterRules } from './types.js';
import { matches } from './rules.js';

type RoutingSubscription = {
  id: string;
  webhook_url: string;
  secret: string;
  delivery_mode: DeliveryMode;
  rules: FilterRules;
};

// The dispatcher supplies an open transaction; routing and completion commit together.
export async function processRouting(client: PoolClient, eventId: string): Promise<boolean> {
  const job = (await client.query<{ status: string }>(
    'SELECT status FROM fanout_jobs WHERE event_id=$1 FOR UPDATE', [eventId],
  )).rows[0];
  if (!job) throw new Error('Routing job not found');
  if (job.status === 'done') return false;
  const event = (await client.query<EventInput & { id: string }>(
    'SELECT id,type,source,payload FROM events WHERE id=$1', [eventId],
  )).rows[0];
  if (!event) throw new Error('Routing event not found');
  const subscriptions = await client.query<RoutingSubscription>(
    'SELECT id,webhook_url,secret,delivery_mode,rules FROM subscriptions WHERE active=true',
  );
  for (const subscription of subscriptions.rows) {
    if (!matches(event, subscription.rules)) continue;
    await client.query(`INSERT INTO deliveries(id,event_id,subscription_id,webhook_url,secret,delivery_mode)
      VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(event_id,subscription_id) DO NOTHING`,
      [randomUUID(), event.id, subscription.id, subscription.webhook_url, subscription.secret, subscription.delivery_mode]);
  }
  await client.query("UPDATE fanout_jobs SET status='done',locked_until=NULL WHERE event_id=$1", [event.id]);
  return true;
}
