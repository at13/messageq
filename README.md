# MessageQ

A TypeScript/Fastify REST service with PostgreSQL-backed durable ingestion,
subscription matching, signed webhook delivery, retries, and queryable audit history.
See the implemented flow and failure boundaries in [ArchitectureDiagram.md](./ArchitectureDiagram.md).

## Setup

Requirements: a supported, patched Node 22 release (CI/deployment: **22.23.5**),
npm, and PostgreSQL 16+. All development belongs under `/workspaces/messageq`.
An Ubuntu 24 VS Code devcontainer configuration is provided in
[.devcontainer](./.devcontainer); it requires Docker and a reachable PostgreSQL server.
The connected machine is currently a VM, not an IDE-attached Docker container.

```bash
cd /workspaces/messageq
npm ci
cp .env.example .env
# Edit DATABASE_URL and API_TOKEN. Generate a token with: openssl rand -hex 32
# Keep .env private: chmod 600 .env
npm run build
node --env-file=.env dist/migrate.js
node --env-file=.env --import ./dist/telemetry.js dist/main.js
```

`API_TOKEN` is required and must have at least 32 characters. `.env` is **not**
automatically loaded by npm commands; `npm run dev` explicitly loads it.
Run migrations once as an operator, before starting/upgrading workers. Startup
does not run DDL. Use separate migration-owner and application database roles in
deployment; the application needs table DML, not schema ownership.
Example database credentials are for isolated development/CI only.
Review/deactivate legacy subscriptions created while the earlier demo was
unauthenticated before sending real data; new authentication does not retroactively
establish ownership or trust in old registered destinations.

## Public deployment and local testing

The connected deployment is **https://143.110.253.222**. Nginx terminates trusted
TLS and proxies to the private API port. HTTP redirects to HTTPS; do not send a
bearer token over public HTTP. The service runs as a non-root systemd user.
Deployment database and API credentials are generated privately, not committed.

- [Nginx configuration](./deploy/messageq.nginx.conf)
- [Application supervision](./deploy/messageq.service)
- [Hourly certificate renewal](./deploy/messageq-cert-renew.timer)
- [Private Prometheus monitoring](./deploy/prometheus.yml) and [alerts](./deploy/alerts.yml)

The IP certificate is short-lived (six days). Renewal has been dry-run tested;
the timer reloads Nginx after renewal. Adjust paths, address and certificates for
another deployment. Port 3000 and the monitoring port must stay private.

From your **own computer**, clone this repository, securely obtain the deployment
token from the operator, then run:

```bash
export API_TOKEN='<private-token>'
node test-client.mjs https://143.110.253.222
# Optional end-to-end receipt/retry checks when the demo viewer is enabled:
node test-client.mjs https://143.110.253.222 --viewer
```

The default client checks ingress, validation, idempotency and audit response shape.
`--viewer` additionally checks all three demo modes, actual receipt commits and
503-to-204 retry history, then deactivates its subscriptions. Neither mode proves
arbitrary external delivery guarantees. A test run on the remote
host against its public IP is not a test executed on your local computer.
Tokens must never be committed, included in URLs, or pasted into issue/CI logs.

## API

Every management, event, audit and metrics request requires
`Authorization: Bearer <API_TOKEN>`. This is a **single-tenant operator API**:
token holders share management access. `X-Producer-Id` namespaces idempotency
keys; it is not an authenticated tenant identity.

```bash
export BASE_URL=https://143.110.253.222
curl --fail "$BASE_URL/subscriptions" \
  -H "Authorization: Bearer $API_TOKEN"

curl --fail -X POST "$BASE_URL/subscriptions" \
  -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"webhookUrl":"https://receiver.example/hook","secret":"replace-with-a-strong-signing-secret","deliveryMode":"at_least_once","rules":{"type":"order.created","source":"checkout","payload":{"customer.tier":"gold"}}}'

curl --fail -X POST "$BASE_URL/events" \
  -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123-created' -H 'X-Producer-Id: checkout' \
  -d '{"type":"order.created","source":"checkout","payload":{"customer":{"tier":"gold"},"orderId":"123"}}'
```

`type` and `source` are nonempty strings, at most 256 characters. `payload` is
required and may be any JSON value: object, array, string, number, boolean or null.
Request bodies are limited to 1 MiB. Header identifiers are limited to 256 characters.
Signing secrets are 16-256 characters and are omitted from management/audit responses.

| Operation | Endpoint |
|---|---|
| Accept an event after durable commit | `POST /events` → `202` |
| Create/list/deactivate subscriptions | `POST /subscriptions`, `GET /subscriptions`, `DELETE /subscriptions/:id` |
| Event/subscription delivery history | `GET /events/:id/deliveries`, `GET /subscriptions/:id/deliveries` |
| Individual delivery state | `GET /deliveries/:id` |
| Individual attempt history | `GET /deliveries/:id/attempts` |
| Public process/database health | `GET /health/live`, `GET /health/ready` (also HEAD) |
| Authenticated Prometheus metrics | `GET /metrics` |

List endpoints accept `limit` (1-100, default 50) and `offset` (0-100000).
Delivery history includes the latest 100 attempts per delivery; use the separate
attempts endpoint to paginate the full history. UUID/pagination errors return
`400`; missing resources return `404`. Deactivation excludes a subscription from
future routing; already-created deliveries continue.

### Ingestion failures

| Failure | Response / producer action |
|---|---|
| Missing/invalid bearer token | `401`; correct credentials |
| Missing headers, invalid envelope or malformed JSON | `400`; correct the request |
| Oversized body / unsupported media type | `413` / `415`; correct size/type |
| Same producer/key with different content | `409`; use the original content or a new logical key |
| Proxy rate limit | `429`, `Retry-After: 5`; retry with jitter |
| Proxy cannot reach API / upstream timeout | `502` / `504`; acceptance may be unknown, retry the same key |
| Unfinished event queue full | `503`, `EVENT_QUEUE_FULL`, `Retry-After: 5`; retry the same key |
| Database unavailable, exhausted resources or retryable transaction failure | `503`, `SERVICE_UNAVAILABLE`, `Retry-After: 5` |
| Unexpected internal failure | `500`, sanitized error and request ID; investigate and retry the same key |
| Connection lost before response | Outcome unknown; retry the same key, never assume rejection |

An event and its fanout job commit atomically. `202` means **stored, not delivered**.
Queue-full rejection persists neither record. A lost response can still follow a
successful commit; idempotent retry returns the original ID. Object-key order
does not affect content comparison.

`EVENT_QUEUE_CAPACITY` (default 10000, range 1-1000000) counts pending/processing
fanout jobs, **not** retained events, payload bytes or webhook backlog. Idempotency,
capacity check and insertion are serialized by a transaction-scoped advisory
lock across replicas. Matching retries succeed even when full. Routing completion
releases capacity. All replicas must use the same capacity and admission protocol.

## Filter rules

```json
{"type":"order.created","source":"checkout","payload":{"customer.tier":"gold","items.0.sku":"ABC","flags":["paid","checked"]}}
```

All supplied conditions are ANDed. Type/source are exact string matches.
Payload paths are dot-separated own-property paths, including array indices.
Values use structural JSON equality: object key order is irrelevant; array order
and value types matter. Missing differs from null. `{}` matches everything.
Prototype-related paths and empty path segments are rejected. There are no
wildcards, comparisons, executable expressions, OR conditions, or subscription updates.

## Delivery contract

| Mode | Behavior and boundary |
|---|---|
| `at_most_once` | One durable send authorization, consumed before HTTP; never restored or retried. Crash/timeout can lose delivery or leave an unknown effect. |
| `at_least_once` | Retry transport failures, unknown outcomes, `408`, `425`, `429`, and `5xx`. Requests may duplicate. Eventual receipt requires durable sender state, continuing workers and eventual receiver acceptance. |
| `exactly_once` | Same retried transport, plus **receiver transactional deduplication**. Registration requires `receiverSupportsIdempotency: true`; the declaration cannot prove receiver correctness. |

Any HTTP `2xx` acknowledges delivery; redirects are never followed. Other `4xx`
and redirects fail permanently. Retries use persisted exponential backoff with
50-100% jitter (base 500ms, capped at 60s), honoring `Retry-After` up to 60s.
There is no finite retry budget for recoverable failures.
Lease tokens fence stale results. Authorized attempts are persisted before
network I/O; expired attempts become `unknown`, never fabricated HTTP success.
Completed success cannot be overwritten by an old worker.

Webhooks must be HTTPS without URL credentials and resolve only to public
unicast addresses. DNS is checked at registration **and every attempt**; connections
pin a validated address while preserving TLS hostname verification. Mixed/public-private,
loopback, link-local and IPv4-mapped private destinations are blocked. HTTP response
bodies are not retained; the deadline covers DNS and HTTP.

The envelope contains `deliveryId`, `eventId`, `type`, `source`, and `payload`.
`X-MessageQ-Delivery-Id` is stable across retries. Verify
`X-MessageQ-Signature-V2 = hex(HMAC-SHA256(secret, timestamp + "." + rawBody))`
with `X-MessageQ-Timestamp`, a bounded clock-skew window and constant-time comparison.
Legacy `X-MessageQ-Signature` signs the raw body without a timestamp and is retained
for compatibility with the demo receiver.

For exactly-once **business effects**, the receiver must insert the delivery ID
and apply its business mutation in one database transaction, acknowledge only
after commit, and durably retain deduplication records. External nontransactional
effects need their own idempotency protocol. The sender cannot promise exactly-once HTTP.

## Browser test viewer

Enable `TEST_VIEWER_ENABLED=true`; otherwise the viewer is unavailable.
Open **https://143.110.253.222/test-viewer/**:

1. Enter the private API token and click **Connect**.
2. Create a subscription with rules and one of the three delivery modes.
3. Optionally fail the first N requests to observe retries and audit codes.
4. Send matching JSON and inspect received receipts, delivery state and attempts.
5. Disable test subscriptions, then **Disconnect** to clear browser credentials/session.

The token stays in memory, not browser storage. Only the session ID uses session
storage. The signed receiver is a fixed server-side loopback route; users cannot
register arbitrary internal URLs. Receipts deduplicate transactionally in PostgreSQL
and survive restarts. The demo accepts the legacy body signature, illustrating a
database receipt as the business effect, not arbitrary external effects.
Viewer sessions/data have no automatic expiry or retention; keep it disabled for production.

## Tests and CI

```bash
# Isolated PostgreSQL database only; tests migrate and clean up their fixtures.
DATABASE_URL=postgres://messageq_test:messageq_test@localhost:5432/messageq_viewer_test npm test
npm run test:unit
# Use the same isolated DATABASE_URL for npm run test:integration.
npm run typecheck
npm run build
npm audit --omit=dev
```

CI provides its own PostgreSQL service and test-only bearer token. It runs
installation, typecheck, build, dependency audit, migration and all tests on
**every branch push and every pull request**. Protected `main` requires an up-to-date
successful `test` check, including administrators; publish through PRs.
See [TestingStrategy.md](./TestingStrategy.md) for executed coverage and limitations.

### Bounded load tests

```bash
API_TOKEN='<private-token>' npm run load:test -- \
  --url https://143.110.253.222 --duration 5 --rate 2 --concurrency 2
# Higher load only on an isolated, authorized environment:
API_TOKEN='<private-token>' npm run load:test -- \
  --url http://127.0.0.1:3000 --duration 60 --rate 100 --concurrency 16 \
  --max-error-rate 0.01 --max-p99 500 --output load-result.json
```

[The runner](./tests/load-test.ts) bounds rate, concurrency, duration and request
count, rejects plaintext authenticated remote URLs, and never follows redirects.
It reports status counts, acceptance validity, throughput, latency percentiles and
threshold failures. Nonzero exit means failure; report writes never overwrite files.
It is closed-loop: it slows under saturation and does not measure coordinated-omission
latency. Ingestion `202` is not evidence of delivery throughput. Public Nginx limits
each source to 10 RPS; do not disable shared-host protections to improve a benchmark.
The proxy also limits each source to 20 active connections and bounds idle header/body waits.

## Observability and operations

Structured logs correlate request IDs and delivery IDs without authorization or
signing secrets. Metrics cover HTTP requests/latency, delivery outcomes/latency,
fanout/delivery backlog and oldest pending delivery, plus process metrics.
Private Prometheus runs on `127.0.0.1:9090`, with two-day/256MiB retention and
availability, backlog and HTTP-error alerts. Rules are validated with `promtool`;
configure Alertmanager routing before relying on external paging.

Optional HTTP/PostgreSQL traces preload via `dist/telemetry.js`. Set
`OTEL_EXPORTER_OTLP_ENDPOINT` only to an **operator-controlled** OTLP HTTP collector
(e.g. `http://127.0.0.1:4318`). No external trace collector is configured on the demo.
SIGTERM stops claiming, drains work, and closes HTTP, the pool and telemetry.

## What we sacrifice for simplicity vs. what we'd harden next

**Intentional simplicity:** one PostgreSQL database is both queue and system of
record; admission uses a global lock; fanout scans active subscriptions and commits
each event atomically without subscription versions or incremental checkpoints.
Workers run bounded, nonoverlapping batches rather than an independently deployed
worker fleet. Equality-only rules, offset pagination and a single operator token
keep the contract small.

**Next production hardening:** high-availability PostgreSQL with tested off-host
backup restoration/RPO, separate tenant identities and quotas, encrypted secrets
and rotation, retention/storage limits, large-fanout checkpoints, endpoint isolation
and circuit breakers, alert delivery, and destructive process/host failover tests.
Local durable commit is not protection against unrecoverable loss of the database disk.

**Excluded extensions:** DigitalOcean managed-queue deployment, per-source ordered
delivery, and replay/redrive APIs are not implemented. Existing state/audit guarantees
do not imply ordering, disaster-proof durability or unconditional eventual receipt.

Before workstation handoff, sign out of GitHub CLI and personal editor/browser
accounts and disconnect the viewer. CLI sign-out does not sign out VS Code/Cursor.
