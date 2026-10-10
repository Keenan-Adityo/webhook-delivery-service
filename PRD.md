# PRD: Webhook Delivery Service (Go)

## 1. Overview

An internal service that accepts events from producers and reliably delivers them over HTTP POST to endpoints that customers have registered. Customer endpoints can be slow, erroring or down for hours, and the service must not lose their events or let one bad endpoint hurt other customers.

The brief is deliberately underspecified. The goal of this project is a small, well-reasoned solution with clear trade-offs, not an exhaustive one. Every decision below must be defensible in the interview.

## 2. Goals and non-goals

### Goals

- Never lose an accepted event, including across a process crash or restart.
- Deliver each event at least once to the customer's registered endpoint.
- Retry failed deliveries with backoff until a deadline, then end in a clear terminal state.
- Let anyone query the delivery status of an event.
- Isolate customers from each other during outages.

### Non-goals (out of scope per the brief)

- Authentication or login, multi-tenant billing, production deployment.
- A UI. The brief says no UI; a status API is enough, so no React frontend is built.
- Multi-instance or distributed operation (single process only; noted as a next step).
- Real customer endpoints. Endpoints are mocked.

## 3. Functional requirements

| ID | Requirement |
|----|-------------|
| FR1 | `POST /events` accepts a customer/subscription identifier and a JSON payload, persists the event, and returns `202 Accepted` with an `event_id` without waiting for delivery. |
| FR2 | Invalid input (unknown subscription, malformed JSON, oversized payload) is rejected with a 4xx and nothing is stored. |
| FR3 | A background delivery loop sends each event to the subscription's endpoint via HTTP POST with a per-request timeout. |
| FR4 | A 2xx response marks the event delivered. Any other response, network error or timeout schedules a retry. |
| FR5 | Retries use exponential backoff with jitter, a maximum delay and an overall retry deadline. |
| FR6 | When the deadline passes, the event moves to terminal `failed`. It is kept, never deleted. |
| FR7 | `GET /events/{id}` returns status (`pending`, `delivered`, `failed`), attempt count, next attempt time and last error. |
| FR8 | `GET /events?subscription_id=&status=` lists events for inspection (stretch if time is short). |
| FR9 | Pending events survive a restart and resume delivery. |
| FR10 | Subscriptions (customer id plus endpoint URL) can be registered through a simple endpoint or seed config. No auth. |

## 4. Key design decisions

### 4.1 Delivery semantics: at-least-once

- The event is persisted before the producer gets a response. Delivery is asynchronous.
- A timeout or lost response cannot be distinguished from a failure, so it is retried. This means duplicates are possible.
- **Cost:** duplicates. **Demand on the customer:** idempotent handlers, using `event_id` as the idempotency key.
- **Why not at-most-once:** it can silently lose events, which contradicts the core requirement.
- **Why not exactly-once:** it is not achievable over HTTP without cooperation from the receiver.

### 4.2 Retry behavior

Proposed defaults, all configurable:

| Parameter | Default |
|-----------|---------|
| Per-request timeout | 10s |
| Initial delay | 10s |
| Multiplier | 2x |
| Jitter | randomized (for example +/-20%) to avoid a thundering herd on recovery |
| Maximum delay | 30 min |
| Retry deadline | 24h from event creation |

- Retry on: network errors, timeouts, 5xx, 408, 429 (respect `Retry-After` if present).
- Do not retry endlessly on permanent client errors such as 4xx other than 408/429. Mark failed early, and record the reason.
- Terminal state: `failed` after the deadline, retained with its full attempt history so it can be replayed manually.

### 4.3 Long outages: shared worker pool with bounded concurrency

Chosen over per-customer processing for simplicity and bounded resource use.

- A fixed pool of N workers (default 8) claims due deliveries from the database, oldest first.
- A failed event is just a row with a future `next_attempt_at`. A worker is never parked waiting on a dead endpoint, so a six-hour outage consumes no workers between attempts.
- Per-customer concurrency cap (default 2 in-flight) so a slow or backlogged customer cannot occupy the whole pool.
- Per-endpoint circuit breaker: after K consecutive failures (default 5), pause that endpoint for a cool-down that grows over time, instead of burning retries on every event.
- Short request timeout so slow endpoints release workers quickly.
- On recovery, the backlog drains in parallel within the per-customer cap.

**Trade-offs:** less isolation than dedicated per-customer queues, and no ordering from the pool itself. In exchange we avoid per-customer goroutine lifecycle management and keep the design small. Per-customer queues are listed as a next step.

### 4.4 Ordering: no strict guarantee

- Workers pick the oldest due event first, so delivery is roughly chronological in the normal case. Retries and concurrent workers can still reorder events.
- Each delivery carries `event_id`, `created_at` and a per-customer `sequence` so consumers can dedupe and reorder.
- **Why not strict ordering:** it requires per-customer serialization, which lets one failing event block all later events for that customer during an outage.
- **Cost:** customers must tolerate out-of-order and duplicate deliveries.
- **Next step:** an opt-in ordered mode per subscription, with a limit on how long a blocked event can hold the queue.

## 5. Architecture

Single Go process with three parts:

1. **HTTP API:** ingest, status and subscription endpoints.
2. **Datastore:** source of truth for events, subscriptions and attempts. Proposed: SQLite (pure-Go driver) for a zero-setup one-line run, with transactional claims. Postgres is the production-like alternative using `FOR UPDATE SKIP LOCKED`; the cost is needing Docker to run.
3. **Dispatcher and workers:** a scheduler polls for due events and hands them to the worker pool, which performs the HTTP calls and records each attempt.

**Flow:** producer posts event, API validates and stores it as `pending` with `next_attempt_at = now`, API returns 202. A worker claims the row with a lease, POSTs to the endpoint, then marks delivered, or schedules the next attempt, or marks failed past the deadline.

**Crash safety:** a claim sets a lease expiry. If the process dies mid-delivery, the lease expires and the event is picked up again. This is consistent with at-least-once.

**Graceful shutdown:** stop claiming, let in-flight requests finish within a timeout, then exit.

## 6. Data model

```
subscriptions:     id, customer_id, endpoint_url, created_at
events:            id, subscription_id, sequence, payload (JSON), status,
                   attempt_count, next_attempt_at, lease_until, last_error,
                   created_at, delivered_at
delivery_attempts: id, event_id, attempted_at, http_status, error, duration_ms
```

Index on `(status, next_attempt_at)` for the dispatcher query.

## 7. API

### `POST /events`

Request:

```json
{ "subscription_id": "sub_123", "payload": { } }
```

Response `202`:

```json
{ "event_id": "evt_...", "status": "pending" }
```

### `GET /events/{id}`

Response `200`: event status, `attempt_count`, `next_attempt_at`, `last_error`, and the attempt history.

### Outgoing delivery (POST to customer endpoint)

- **Body:** `{ "event_id", "created_at", "sequence", "payload" }`
- **Headers:** `X-Event-Id`, `X-Delivery-Attempt`, `Content-Type: application/json`

## 8. Non-functional requirements

- **Durability:** an event is stored before the 202 is returned.
- **Resilience:** one endpoint's failure must not stop deliveries to others.
- **Resource bounds:** fixed worker count, request timeouts, payload size limit.
- **Observability:** structured logs for each attempt, a `/healthz` endpoint, and simple counters (pending, delivered, failed).
- **Simplicity:** one-line run command, for example `go run ./cmd/server`.

## 9. Test plan

A mock customer endpoint tool (a small Go HTTP server) with switchable behaviors: always 200, always 500, slow response beyond the timeout, intermittent failure, and "down for N minutes then recovers".

Key scenarios:

- **Happy path:** event delivered once, status `delivered`.
- **Endpoint down then recovers:** the event is eventually delivered with backoff visible in the attempt history.
- **Simulated long outage** (backoff and deadline times scaled down via config): other customers' deliveries stay timely, the breaker opens, and the stuck events end `failed` after the deadline.
- **Slow endpoint:** workers are released at the timeout, and the per-customer cap holds.
- **Crash mid-delivery:** after restart, the leased event is retried (duplicate is acceptable).
- **Unit tests** for backoff calculation and retry classification.

## 10. Plan

| Block | Output |
|-------|--------|
| Setup, schema, subscriptions | Project skeleton, DB, migrations |
| Ingest and status API | `POST /events`, `GET /events/{id}` |
| Dispatcher, workers, retry and backoff | End-to-end delivery with retries |
| Per-customer cap and circuit breaker | Isolation during outages |
| Mock endpoint and tests | Scenarios from section 9 |
