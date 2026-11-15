# Testing Strategy

This strategy validates the event ingestion and notification fanout service described in [ArchitectureDiagram.md](./ArchitectureDiagram.md).

Use unit tests for deterministic decisions and integration tests for database transactions, durable recovery, multi-node coordination, and HTTP behavior. The highest-value tests exercise crash boundaries, not just successful delivery.

## Integration Test Environment

Run the real API and workers against an isolated, real database and a controllable webhook server. Do not mock database transactions or worker coordination.

For exactly-once processing tests, use a receiver with a real transactional inbox and business table. A mock returning HTTP `200` cannot establish exactly-once processing.

The webhook server should support configurable responses, connection failures, delayed responses, and processing followed by acknowledgement loss. Record requests and committed business effects independently.

Introduce explicit failure hooks at transaction and network boundaries. Coordinate concurrent workers with barriers rather than relying on arbitrary sleeps. Isolate test data and perform deterministic cleanup without affecting shared services.

## Integration Test Coverage

| Area | Essential scenarios and assertions |
|---|---|
| Durable ingestion | Accepted event and fanout job survive restart. A failed transaction leaves neither record. No acceptance response is issued before commit. |
| Ingestion idempotency | Lose the API response after commit, then retry with the same key: return the same event ID and create only one event/job. Reuse the key with different content: return a conflict. Verify key scope per producer. |
| Subscription lifecycle | Create, list, and delete without restart. Verify tenant isolation. Rule changes and deletion during routing preserve the captured subscription-version snapshot. |
| Matching and fanout | Type, source, and payload conditions select the correct consumers. No-match events complete routing. Interrupted batched fanout resumes without missing or duplicate delivery records. |
| Multi-node coordination | Multiple workers race for the same work. Unique delivery records are preserved. Lease expiration recovers work according to the snapshotted delivery policy. |
| Webhook delivery | Verify envelope, stable event/delivery IDs, and signatures. A slow or failing consumer does not block healthy consumers. Verify endpoint concurrency limits. |
| Retry behavior | Timeouts, connection failures, `429`, and retryable `5xx` schedule retries. Permanent errors follow policy. Retry times, bounded `Retry-After`, and budget exhaustion are persisted. |
| Audit | Verify attempt timestamps, actual HTTP codes when received, categorized errors, and final status. Unknown outcomes remain distinguishable. Event/subscription queries, tenant authorization, and cursor pagination return correct history. |
| Security | Deny cross-tenant access. Block private/internal webhook destinations and unsafe redirects. Validate signatures against tampered payloads and expired timestamps. |

## Delivery-Guarantee Integration Tests

### At-Most-Once

| Scenario | Required assertions |
|---|---|
| Crash after send authorization commits but before HTTP send | Restart workers: zero HTTP requests, consumed authorization remains consumed, no retry. |
| Receiver processes request; worker crashes before persisting outcome | Restart workers: exactly one HTTP request total, no resend, outcome recorded as unknown. |
| Timeout, connection failure, or HTTP failure | At most one sender-issued attempt, no retry, failure/unknown metadata accurately exposed. |
| Duplicate work item or expired lease | No additional send authorization or HTTP attempt. |
| Attempt to redrive the same logical delivery | Reject redrive; a new explicitly authorized delivery is outside the original delivery's guarantee. |

Disable automatic HTTP client retries and verify that the test transport does not introduce retries. At-most-once tests assert sender request counts, not guaranteed receiver processing.

### At-Least-Once

| Scenario | Required assertions |
|---|---|
| Receiver fails twice, then succeeds | Retry with the same delivery ID; final status delivered; all attempts recorded. |
| Receiver processes request but drops acknowledgement | Another request arrives with the same delivery ID; duplicate processing is possible. |
| Worker crashes after receiving success but before persisting it | Recovery retries; no lost logical delivery. |
| Retry budget exhausted | Terminal failure and full attempt history; no claim of successful receipt. |
| Restart during backoff | Persisted next-attempt time survives restart; work is not silently dropped. |
| Explicit redrive after terminal failure | Preserve delivery identity and prior attempt history. |

Finite tests cannot prove unconditional eventual delivery. They validate recovery and retry mechanisms under controlled failures. Eventual acknowledgement still depends on continued retries and a receiver that eventually accepts the request.

### Exactly-Once Processing

Use a compatible receiver whose inbox and business changes commit in the same database transaction.

| Scenario | Required assertions |
|---|---|
| Concurrent requests with the same delivery ID | One committed business change; duplicates receive the recorded successful acknowledgement. |
| Receiver crashes before transaction commit | Inbox and business changes roll back; retry produces one committed business change. |
| Receiver commits, then drops acknowledgement | Multiple HTTP requests, exactly one committed business change. |
| Sender restarts and retries | Durable receiver deduplication prevents another business change. |
| Receiver restarts after commit | Deduplication survives restart and acknowledges duplicates without repeating processing. |
| Incompatible receiver configuration | Reject exactly-once mode instead of silently downgrading. |
| Retry/redrive within the supported retention horizon | Retained deduplication record prevents repeated business effects. |

Assert both HTTP request counts and committed business effects. Repeated requests are permitted; repeated committed business effects are not.

The guarantee is limited to the compatible receiver's transaction. External side effects need their own idempotency or transactional outbox protocol and corresponding tests. Loss or expiration of inbox records invalidates deduplication beyond the supported retention horizon.

## Unit Test Coverage

Unit tests should be fast and deterministic, with no database or network dependency.

| Component | Focused cases |
|---|---|
| Event and subscription validation | Missing or invalid envelope fields, payload constraints, unsupported filter operators, and invalid delivery modes. |
| Filter evaluator | AND/OR semantics if supported, nested paths, missing versus `null`, type mismatches, numeric boundaries, and combined type/source/payload conditions. |
| Retry policy | Retryable versus permanent outcomes, exponential growth, maximum delay, jitter bounds, `Retry-After`, and attempt/age limits. |
| Delivery state machine | Allowed transitions, at-most-once authorization cannot be restored, and stale attempt results cannot overwrite recorded success. |
| Idempotency logic | Equivalent content versus conflicting key reuse according to the documented comparison rules. Database uniqueness is an integration concern. |
| Webhook signing | Known signature vectors, tampered payloads, timestamp validation boundaries, and secret rotation. |
| Receiver processing decisions | New versus duplicate delivery decisions. Transactional atomicity remains an integration concern. |

Do not write tests that merely restate implementation details or test framework behavior. Add focused cases for introduced behavior and meaningful edge cases.

## Determinism and Assertions

- Inject a clock and random source so retry-policy unit tests can check exact timing and jitter bounds without sleeping.
- For real worker scheduling, use controlled scheduling hooks or bounded waits tied to observable state.
- Exercise committed crash boundaries with actual worker termination/restart where practical; exceptions alone may invoke cleanup paths unavailable during a crash.
- Assert persisted state after restart, not only in-memory results.
- Assert request counts, unique delivery IDs, committed business effects, and audit history, not just API response codes.
- Distinguish absent HTTP responses from actual HTTP error codes.
- If persisting an outcome fails, verify policy-aware recovery and an unknown audit outcome rather than fabricated success.
- Keep data and infrastructure isolated, especially when running on shared environments.

## CI and Load Testing

- Run unit tests on every change.
- Run isolated database/webhook integration tests in CI, using the smallest relevant selection during development.
- Keep load and resilience tests separate from the normal unit suite.
- Define throughput and latency targets before load testing; measure ingestion throughput, routing lag, oldest pending delivery, and audit-write performance against those targets.
- Test backlog recovery after worker outages and isolation when one consumer is slow or unavailable.
- Run multiple API and worker instances to expose contention and verify policy-aware coordination.
- Validate database backup restoration and failover separately against the promised durability objective.

## Implemented Viewer and Load Tests

- [tests/integration.test.ts](./tests/integration.test.ts) verifies `503 EVENT_QUEUE_FULL` and `Retry-After`, absence of rejected records, idempotent/conflicting replays while full, simultaneous admissions from separate API instances, and capacity release after routing.
- The load runner is in [tests/load-test.ts](./tests/load-test.ts); [tests/load-test.test.ts](./tests/load-test.test.ts) covers bounded concurrency, unique keys, rate/duration limits, request caps, malformed/duplicate acceptance responses, HTTP/network errors, latency thresholds, and interrupted runs.
- [tests/viewer.integration.test.ts](./tests/viewer.integration.test.ts) uses real PostgreSQL and a listening Fastify server to verify gated viewer availability, session-scoped management, signed webhook receipts, simulated retries, at-most-once failure, and concurrent transactional receipt deduplication.
- Set `DATABASE_URL` to an isolated test database. Tests clean up their own events, subscriptions, and sessions; do not run them against production.
- Viewer tests demonstrate a transactional database receipt as the business effect. They do not prove arbitrary external exactly-once effects or complete production crash/failover guarantees.

## Initial Priority

Start with:

1. Durable acceptance and producer idempotency.
2. Correct matching and interrupted fanout recovery.
3. Multi-worker coordination.
4. Crash-boundary tests for all three delivery modes.
5. Accurate and authorized delivery audit history.

These tests establish the core guarantees before expanding to load and broader resilience testing.
