import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

async function main() {
  if (process.argv.length > 4 || (process.argv[3] && process.argv[3] !== '--viewer')) throw new Error('Usage: node test-client.mjs [HTTPS_URL] [--viewer]');
  const baseUrl = new URL(process.argv[2] ?? 'https://143.110.253.222');
  if (!process.env.API_TOKEN) throw new Error('API_TOKEN is required');
  assert.ok(['http:', 'https:'].includes(baseUrl.protocol), 'Expected an HTTP or HTTPS service URL');
  if (baseUrl.protocol !== 'https:' && !['127.0.0.1', 'localhost', '[::1]'].includes(baseUrl.hostname)) throw new Error('Use HTTPS for remote authenticated testing');
  const key = randomUUID();
  const producer = `public-smoke-${key}`;
  const event = { type: `public.smoke.${key}`, source: 'test-client', payload: { test: true } };

  async function request(path, method = 'GET', body, headers = {}) {
    const response = await fetch(new URL(path, baseUrl), {
      method,
      redirect: 'error',
      headers: { ...(body === undefined ? {} : { 'content-type': 'application/json' }), authorization: `Bearer ${process.env.API_TOKEN}`, ...headers },
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
  if (process.argv[3] === '--viewer') {
    const session = await request('/test-viewer/sessions', 'POST');
    assert.equal(session.status, 201, 'Viewer session should be created');
    const viewerHeaders = { 'x-test-viewer-session': session.data.id };
    const subscriptionIds = [], eventIds = [], expectedAttempts = new Map();
    try {
      for (const deliveryMode of ['at_most_once', 'at_least_once', 'exactly_once']) {
        const type = `viewer.smoke.${key}.${deliveryMode}`;
        const subscription = await request('/test-viewer/subscriptions', 'POST', {
          deliveryMode, rules: { type }, failFirst: deliveryMode === 'at_most_once' ? 0 : 1,
        }, viewerHeaders);
        assert.equal(subscription.status, 201, 'Viewer subscription should be created');
        subscriptionIds.push(subscription.data.id);
        const sent = await request('/events', 'POST', { type, source: 'test-client', payload: [null, deliveryMode] },
          { 'idempotency-key': `${key}:${deliveryMode}`, 'x-producer-id': session.data.id });
        assert.equal(sent.status, 202);
        eventIds.push(sent.data.id);
        expectedAttempts.set(sent.data.id, deliveryMode === 'at_most_once' ? 1 : 2);
      }
      const deadline = Date.now() + 30_000;
      let state;
      while (Date.now() < deadline) {
        const result = await request('/test-viewer/state', 'GET', undefined, viewerHeaders);
        assert.equal(result.status, 200);
        state = result.data;
        if (eventIds.every(id => state.deliveries.some(delivery => delivery.event_id === id && delivery.status === 'delivered'))) break;
        await new Promise(resolve => setTimeout(resolve, 600));
      }
      for (const id of eventIds) {
        const delivery = state?.deliveries.find(item => item.event_id === id);
        assert.equal(delivery?.status, 'delivered', 'Notification must reach the demo receiver before timeout');
        assert.equal(delivery.attempt_count, expectedAttempts.get(id));
        const receipt = state.notifications.find(item => item.delivery_id === delivery.id);
        assert.ok(receipt, 'A committed receiver receipt is required');
        assert.equal(receipt.receive_count, 1, 'Failure simulation must not commit a receiver effect');
        const attempts = await request(`/deliveries/${delivery.id}/attempts`);
        assert.equal(attempts.status, 200);
        assert.equal(attempts.data.at(-1).response_code, 204);
        if (expectedAttempts.get(id) > 1) assert.equal(attempts.data[0].response_code, 503);
      }
      console.log('PASS: all three demo modes delivered; retry codes and committed receipts verified');
    } finally {
      for (const id of subscriptionIds) {
        const removed = await request(`/test-viewer/subscriptions/${id}`, 'DELETE', undefined, viewerHeaders);
        assert.equal(removed.status, 200, 'Test subscriptions must be deactivated');
      }
    }
  }
  console.log(`Smoke tests passed against ${baseUrl.origin}. Created event: ${accepted.data.id}`);
  console.log('Demo receipt checks do not prove arbitrary external exactly-once effects; default mode checks ingress only.');
}

main().catch(error => {
  console.error('Smoke test failed:', error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
