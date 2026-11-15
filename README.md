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

### Connected Remote Host

Set `HOST=127.0.0.1` when deploying for development: the API currently has no authentication and should not be exposed publicly. Pass environment variables to the compiled service, or load your local `.env` using `node --env-file=.env dist/main.js`. `npm start` does not automatically load `.env`.

To test from your local machine, forward remote port 3000 using the VS Code Ports panel, or open an SSH tunnel:

```bash
ssh -N -L 3000:127.0.0.1:3000 <remote-user>@<remote-host>
```

In another local terminal:

```bash
curl --fail http://127.0.0.1:3000/subscriptions
curl --fail -X POST http://127.0.0.1:3000/events \
  -H 'content-type: application/json' \
  -H 'idempotency-key: local-smoke-1' \
  -H 'x-producer-id: local-smoke' \
  -d '{"type":"local.smoke","source":"local-terminal","payload":{"test":true}}'
```

Repeat the second request with the same key and content: it should return the same event ID. Use `GET /events/<event-id>/deliveries` to inspect fanout results; an empty array is expected when no subscription matches.

### Public Nginx Test Deployment

The connected host's test URL is `http://143.110.253.222`. Nginx proxies public port 80 to the Node service on `127.0.0.1:3000`. The configuration is in [deploy/messageq.nginx.conf](./deploy/messageq.nginx.conf); change `server_name` for a different host. Port 3000 need not be opened publicly.

**Temporary testing only:** the API has no authentication or tenant authorization, and this HTTP endpoint has no TLS. Anyone able to reach it can submit events, inspect history, and manage subscriptions. Do not send secrets or production data. Rate limiting reduces accidental load but does not provide access control. Before production, add authentication, authorization, TLS with a domain/certificate, and network restrictions.

From your own computer, copy [test-client.mjs](./test-client.mjs) or clone this workspace and run with Node.js 22.18+:

```bash
node test-client.mjs http://143.110.253.222
```

The client needs no npm dependencies. It checks public reachability, validation, event acceptance, sequential idempotency, conflicting key reuse, and the delivery-history response shape. It creates one uniquely identified event per run and does not delete it, since event deletion is not an API operation. It does not establish webhook delivery guarantees or prove that fanout has completed. A run from the remote host against its public IP is not a test from your local computer.

### Browser Test Viewer

Set `TEST_VIEWER_ENABLED=true` and restart the service to enable `/test-viewer/`. The connected test deployment exposes the viewer at `http://143.110.253.222/test-viewer/`.

1. Open the viewer; it creates a session and proposes a unique event type.
2. Create a subscription with filter JSON and a delivery mode.
3. Optionally set **Fail the first N requests** to observe HTTP `503` retries. At-most-once stops after its first failed request.
4. Send an event with the same type and matching payload.
5. Watch delivery state, view attempt history, and inspect the received notification. Disable subscriptions when finished.

The viewer uses the same ingestion, matching, fanout, signing, delivery, and audit code as other subscribers. It registers only a fixed loopback receiver, never a user-provided internal URL; public webhook URL validation is not relaxed. Sessions isolate the viewer's management/state endpoints with an unguessable session ID, but **this does not add authentication to the rest of the API**.

The demo receiver verifies the HMAC against the raw request body, stores one receipt per delivery ID transactionally, and acknowledges only after commit. Concurrent/repeated requests increment the receipt's receive count without creating another demo business effect. This illustrates exactly-once processing of a database receipt, not arbitrary external side effects. Notifications and sessions survive process restart in PostgreSQL. State lists show the latest 100 records; test-data retention and session expiry are not implemented. Failure simulation is per subscription, not per event.

The viewer is disabled by default and should remain disabled in production. It adds no publicly callable load-generation endpoint. Browser traffic polls at most once every two seconds while visible. As with the public API, use only non-sensitive data on this unauthenticated HTTP test deployment.

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

### Event Queue Full

`EVENT_QUEUE_CAPACITY` limits accepted events awaiting fanout (default `10000`, valid range `1`-`1000000`). Pending and processing fanout jobs occupy slots; completed jobs do not. This is not a limit on retained events, payload bytes, or pending webhook deliveries.

When no slot is available, a **new** event is rejected before persistence:

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 5
Content-Type: application/json
```

```json
{
  "code": "EVENT_QUEUE_FULL",
  "error": "Event queue is full",
  "capacity": 10000,
  "queuedEvents": 10000
}
```

Retry after the indicated delay with jitter and the **same idempotency key**. An already-accepted event still returns `202` with its existing ID even when the queue is full; conflicting reuse returns `409`. JSON object key order does not affect replay comparison.

Admission is coordinated through a PostgreSQL transaction-scoped advisory lock: idempotency lookup, queue count, and event/job insertion are serialized across API replicas. A rejected transaction creates neither event nor job. Configure the same capacity on all replicas and roll out this version consistently; older instances that bypass admission can exceed the limit.

For simplicity, each ingestion counts unfinished jobs under one global lock, with a partial index supporting that query. This trades ingestion parallelism for correctness; replace it with a carefully maintained transactional counter/reservation scheme if measured throughput requires it. A blocked or unavailable database can still cause an internal error or timeout rather than this queue-full response. Queue capacity alone does not bound delivery backlog or storage use.

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

The unit suite covers matching/retry decisions, admission configuration, and load-runner behavior. API integration tests cover queue saturation, retry/conflict behavior, concurrent capacity enforcement, and capacity release. Viewer integration tests use real PostgreSQL and HTTP to cover session isolation, signed receipt persistence, simulated retry, at-most-once failure, and concurrent deduplication. Set `DATABASE_URL` to an isolated test database with no unfinished jobs before running `npm test` or `npm run test:integration`; `.env` is not automatically loaded by npm scripts. Database suites run sequentially because they share that database; individual concurrency tests still issue parallel requests. CI supplies a dedicated PostgreSQL service. Full crash-boundary and failover coverage remains future work.

### CI and Merge Policy

GitHub Actions runs the `test` job on pushes to every branch and pull requests targeting every branch, including typecheck, build, migrations, and the complete test suite. A manual workflow trigger is also available. New pushes cancel older CI runs for the same ref.

The default branch should require the successful `test` status check and up-to-date branches before merge, including administrators. Required-check enforcement is a GitHub branch-protection setting, not something a workflow file can enforce alone. After protection is enabled, use pull requests rather than direct pushes to the default branch.

### Load Testing

The load runner lives in [tests/load-test.ts](./tests/load-test.ts), separate from production source. It uses concurrent HTTP clients, an aggregate rate ceiling, a dispatch-duration limit, and a hard request cap. No extra load-test dependencies are needed.

```bash
# Low-impact smoke load against the deployed Nginx endpoint:
npm run load:test -- --url http://143.110.253.222 \
  --duration 5 --rate 2 --concurrency 2

# On an isolated environment authorized for higher load:
npm run load:test -- --url http://127.0.0.1:3000 \
  --duration 60 --rate 100 --concurrency 16 \
  --max-error-rate 0.01 --max-p99 500 --output load-result.json

npm run load:test -- --help
```

JSON reports include attempted/accepted requests, HTTP status counts, network/timeouts, malformed or duplicate acceptance IDs, achieved throughput, latency min/mean/p50/p95/p99/max, bounded error samples, and threshold results. Exit code is nonzero for a failed threshold, interruption, configuration error, or report-write error. Report files are created exclusively rather than overwriting earlier results. For pure JSON stdout, compile with `npx tsc -p tsconfig.load.json` and invoke `node dist-tests/load-test.js` directly. Normal CI tests the runner against controlled HTTP servers; it never load-tests the public deployment.

Every request has a fresh producer-scoped idempotency key. Defaults are deliberately conservative: 10 seconds, 5 requests/second, and 4 concurrent requests. Use `--payload-bytes` for padding size, and `--event-type` to target existing subscriptions; without it, the run uses a unique event type. Runs retain their accepted events and jobs; there is no event-delete API. Do not load-test production or a shared host at high rates without permission, capacity limits, and a retention plan.

The rate is a ceiling, not a promise: this bounded, closed-loop generator slows down when concurrency is saturated and does not accumulate an unbounded queue of scheduled requests. Latency measures dispatch through response-body consumption, not time waiting for a dispatch slot, so it does not measure open-loop coordinated-omission latency. Throughput includes draining outstanding requests, which may extend beyond the dispatch duration up to the configured timeout. Signals stop scheduling; in-flight requests drain.

**Scope:** a valid `202` with a unique event ID is ingestion success, not proof of completed routing or webhook processing. For fanout/delivery capacity, configure controlled receiver subscriptions in an isolated environment and measure routing lag, delivery backlog, audit records, retry rates, and receiver business effects separately. Nginx currently limits requests to 10 RPS per source IP: higher public-endpoint loads can intentionally produce `429` responses, which are counted as failures rather than retried or hidden. Do not disable that protection on the shared public host just to improve a benchmark.

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
