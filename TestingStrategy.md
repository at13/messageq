# Testing strategy and executed coverage

Tests validate the implemented boundaries in [ArchitectureDiagram.md](./ArchitectureDiagram.md).
Database suites use **real isolated PostgreSQL**, not mocked transactions. Individual
tests coordinate concurrent requests/claims; suites run sequentially because they
share a test database. The viewer receiver uses a transactional PostgreSQL receipt
as its demonstrable business effect.

## Automated coverage

| Suite | Exercised behavior |
|---|---|
| [Unit decisions](./tests/unit.test.ts) | Event JSON types, equality filters/own paths, missing/null/type distinctions, prohibited paths, retryable/permanent outcomes, exponential jitter bounds, Retry-After parsing/capping, capacity validation |
| [REST integration](./tests/integration.test.ts) | Authentication, no unauthorized persistence, arbitrary JSON storage, malformed JSON, media/body limits, UUID/pagination validation, health/metrics, sanitized retryable DB errors, subscription CRUD, exactly-once capability requirement, audit timestamps/codes/pagination and secret exclusion |
| REST admission cases | Queue-full rejection persists nothing, idempotent/conflicting retries while full, semantic object equality, concurrent multi-API admission and shared-key races, capacity release only after routing |
| [Worker/database integration](./tests/worker.integration.test.ts) | Admission/claim rollback, atomic fanout/uniqueness, pre-send audit, connection release, arbitrary JSON, concurrent claims, crash gaps, stale-result fencing, unknown-history recovery, permanent/retryable failures, real HTTP timeout and graceful/nonoverlapping shutdown |
| [Viewer integration](./tests/viewer.integration.test.ts) | Gated availability, authenticated session management, session isolation, signed raw-body receipts, simulated retries, at-most-once failure, concurrent transactional deduplication and receipt persistence |
| [Webhook transport](./tests/webhook.test.ts) | Private/mixed/mapped-private DNS rejection, public-address pinning, URL credential/HTTPS policy, fixed loopback exception, actual wire signatures/envelope, Retry-After cap, no redirect follow or response-body buffering |
| [Load runner](./tests/load-test.test.ts) | Bounded concurrency/rate/duration/request count, unique keys, valid/malformed/duplicate acceptance, HTTP/network errors, thresholds, interruption, plaintext remote bearer rejection |

Expired at-most-once authorization is never restored. Retry-enabled unknowns
recover, while stale success cannot regress a newer terminal outcome. Attempt
history distinguishes missing responses from actual HTTP failure codes.

## Running

```bash
npm ci
npm run typecheck
npm run build
DATABASE_URL=postgres://messageq_test:messageq_test@localhost:5432/messageq_viewer_test npm test
npm run test:unit
# Set the same isolated DATABASE_URL for npm run test:integration.
npm audit --omit=dev
```

The connected host has a separate test role/database. For another environment,
create an isolated database and use its own owner credentials. Never point tests
at deployment data; fixtures migrate and delete their own records. CI provisions
its own PostgreSQL 16 service and injects a test-only token.

## Deployment checks

Beyond the suite, validate:

- Public HTTPS with trusted certificate (never `curl -k`), HTTP redirect, anonymous
  health and unauthorized `401`, plus authenticated ingress/idempotency/audit.
- Supervised non-root application, explicit migration, database access separated
  from schema ownership, private credentials and blocked public database/API ports.
- `nginx -t`, `systemd-analyze verify`, ACME renewal dry-run and enabled hourly timer.
- `promtool check config`, live authenticated scrape and alert rules.
- Bounded public smoke/load runs; report ingestion success separately from routing
  or committed receiver effects. Run a local client on your own computer separately.
- Viewer Connect/create/send/receive/failure simulation, then disable subscriptions
  and Disconnect without retaining a bearer token in storage.

## What these tests do not prove

Finite tests cannot establish unconditional eventual delivery or exactly-once
external effects. Some crash boundaries are recreated from committed database
state rather than destructive process termination. Production disk loss,
replicated database failover, backup restoration/RPO, arbitrary receiver crash
transactions, clock skew, secret rotation, tenant isolation, endpoint circuit
breakers and very large fanouts need separate operational/resilience validation.

The receiver demonstration proves a deduplicated database receipt, not payment,
email or other nontransactional effects. The devcontainer configuration has not
been launched on the connected VM; the IDE remains VM-attached. An optional OTLP
exporter is implemented, but no external collector or alert paging destination is
configured. Managed queues, ordering and replay are deliberately excluded.

CI runs on every branch push and every PR. Required-check enforcement belongs to
GitHub branch protection; a workflow alone cannot enforce merge policy.
