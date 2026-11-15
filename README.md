# MessageQ

MessageQ is a TypeScript event ingestion and webhook fanout service. It accepts structured events, durably matches them to subscriptions, retries deliveries, and exposes delivery audit history.

## Setup

Requirements: Node.js 22.18+, PostgreSQL 16+, and npm.

```bash
cp .env.example .env
npm install
npm run migrate
npm run build
npm test
npm start
```

For local PostgreSQL:

```bash
createdb messageq
DATABASE_URL=postgres://localhost:5432/messageq npm run migrate
```

`POST /events` requires `Idempotency-Key` and accepts `type`, `source`, and arbitrary JSON `payload`. `X-Producer-Id` scopes idempotency keys.

## API

```bash
curl -X POST http://localhost:3000/subscriptions \
  -H 'content-type: application/json' \
  -d '{"webhookUrl":"https://example.com/hook","secret":"at-least-8-characters","deliveryMode":"at_least_once","rules":{"type":"order.created","source":"checkout","payload":{"customer.tier":"gold"}}}'

curl -X POST http://localhost:3000/events \
  -H 'content-type: application/json' \
  -H 'idempotency-key: order-123-created' \
  -d '{"type":"order.created","source":"checkout","payload":{"customer":{"tier":"gold"},"orderId":"123"}}'
```

Audit endpoints:

- `GET /events/:id/deliveries`
- `GET /subscriptions/:id/deliveries`
- `GET /deliveries/:id/attempts`

## Filter Rule Syntax

Rules are JSON:

```json
{
  "type": "order.created",
  "source": "checkout",
  "payload": {
    "customer.tier": "gold",
    "region": "us-east"
  }
}
```

`type` and `source` are exact matches. Payload keys use dot-separated paths and values use strict equality. Missing paths do not match. The current simple syntax intentionally does not support arbitrary expressions, code execution, or complex boolean operators.

## Delivery Guarantees

- `at_most_once`: one sender-authorized HTTP attempt and no retry. A crash or unknown result can lose the notification.
- `at_least_once`: retryable failures and unknown outcomes are retried with exponential backoff. Duplicate requests are possible.
- `exactly_once`: transport still retries, but a compatible receiver must transactionally deduplicate the stable `deliveryId` with its business changes. HTTP requests can repeat; the sender alone cannot guarantee exactly-once external effects.

The current implementation uses PostgreSQL for durable events, fanout jobs, deliveries, and append-only attempts. The worker claims work with row locks and leases. Webhook delivery requires HTTPS and rejects non-unicast destinations.

## Testing

```bash
npm run test:unit
npm run test:integration
npm test
```

The unit suite covers matching and retry decisions. The integration suite exercises the Fastify API. The CI PostgreSQL service runs migrations and the complete suite; crash-boundary and multi-node tests should be expanded as the service hardens.

## What We Sacrifice for Simplicity vs. What We'd Harden Next

### Sacrificed for simplicity

- PostgreSQL is both the durable system of record and work queue; a broker is not required.
- A single worker loop combines fanout and delivery polling.
- Filter rules support exact equality only.
- Authentication and tenant authorization are represented by producer headers and should be replaced with a real identity layer.
- Delivery pagination is basic and the example API does not include rate limiting.

### Harden next

1. Add an outbox or managed queue when throughput and queue contention require it.
2. Add transactional subscription versions and immutable routing snapshots.
3. Add a dedicated delivery worker pool, dead-letter/redrive controls, and per-endpoint circuit breakers.
4. Add full tenant authentication, authorization, quotas, and audit access controls.
5. Add comprehensive crash-injection, concurrent exactly-once receiver, load, and failover tests.
6. Add OpenTelemetry traces, metrics, alerting, secret management, and tested backup restoration.
