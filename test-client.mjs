import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

async function main() {
  const baseUrl = new URL(process.argv[2] ?? 'http://143.110.253.222');
  assert.ok(['http:', 'https:'].includes(baseUrl.protocol), 'Expected an HTTP or HTTPS service URL');
  const key = randomUUID();
  const producer = `public-smoke-${key}`;
  const event = { type: `public.smoke.${key}`, source: 'test-client', payload: { test: true } };

  async function request(path, method = 'GET', body, headers = {}) {
    const response = await fetch(new URL(path, baseUrl), {
      method,
      headers: { 'content-type': 'application/json', ...headers },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(10_000),
    });
    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      throw new Error(`${method} ${path}: HTTP ${response.status}, expected JSON; received ${text.slice(0, 200)}`);
    }
    return { status: response.status, data };
  }

  const subscriptions = await request('/subscriptions');
  assert.equal(subscriptions.status, 200, 'Subscription listing should return 200');
  assert.ok(Array.isArray(subscriptions.data), 'Subscriptions should be an array');
  console.log('PASS: public API reachable; subscription listing');

  const missingKey = await request('/events', 'POST', event);
  assert.equal(missingKey.status, 400, 'Missing idempotency key should be rejected');
  const invalid = await request('/events', 'POST', { type: '' }, { 'idempotency-key': key });
  assert.equal(invalid.status, 400, 'Invalid event should be rejected');
  console.log('PASS: invalid requests rejected');

  const headers = { 'idempotency-key': key, 'x-producer-id': producer };
  const accepted = await request('/events', 'POST', event, headers);
  assert.equal(accepted.status, 202, 'Valid event should be accepted');
  assert.match(accepted.data.id, /^[0-9a-f-]{36}$/i, 'Acceptance should return an event ID');
  const repeated = await request('/events', 'POST', event, headers);
  assert.equal(repeated.status, 202, 'Idempotent retry should be accepted');
  assert.equal(repeated.data.id, accepted.data.id, 'Retry should return the same event ID');
  console.log('PASS: event acceptance and idempotent retry');

  const conflict = await request('/events', 'POST', { ...event, source: 'different-source' }, headers);
  assert.equal(conflict.status, 409, 'Conflicting key reuse should return 409');
  console.log('PASS: conflicting idempotency key rejected');

  const deliveries = await request(`/events/${accepted.data.id}/deliveries`);
  assert.equal(deliveries.status, 200, 'Delivery query should return 200');
  assert.ok(Array.isArray(deliveries.data), 'Delivery history should be an array');
  console.log('PASS: delivery history endpoint');
  console.log(`Smoke tests passed against ${baseUrl.origin}. Created event: ${accepted.data.id}`);
  console.log('This client checks HTTP ingress, not webhook delivery guarantees or completed fanout.');
}

main().catch(error => {
  console.error('Smoke test failed:', error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
