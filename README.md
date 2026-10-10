# Webhook Delivery Service

An internal Go service that accepts events via `POST /events` and reliably
delivers them over HTTP POST to customer-registered endpoints — with retries,
status querying, per-customer concurrency caps and circuit breaking.

Single process, single command, SQLite as the source of truth.

```bash
go run ./cmd/server
```

- Listens on `:8080`, database file `webhook.db` (created on first run).
- Every time-based parameter is an env var with the PRD default — see
  `internal/config/config.go`.

---

## 1. Prerequisites

- Go 1.27+
- Two terminals, `curl`, optionally `jq`

Sanity check:

```bash
go vet ./...
go test ./...
```

## 2. Run it

**Terminal A — the mock customer endpoint** (a fake receiver you can point the
service at):

```bash
go run ./cmd/mockendpoint -mode ok
```

Listens on `:9090` and logs one line per received delivery (event id, attempt,
duration). The mode is fixed at startup — Ctrl-C and restart to change it.

| Flag | Default | Meaning |
|---|---|---|
| `-mode` | `ok` | `ok` \| `fail` \| `slow` \| `flaky` \| `down` |
| `-delay` | `15s` | sleep before answering (`slow`) |
| `-fail-count` | `3` | leading failures before success (`flaky`) |
| `-down-for` | `2m` | how long it stays down (`down`) |

**Terminal B — the service:**

```bash
go run ./cmd/server
```

Check it's alive:

```bash
curl -s localhost:8080/healthz
# {"status":"ok","pending":0,"delivered":0,"failed":0}
```

## 3. Register a subscription

```bash
curl -s -X POST localhost:8080/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"acme","endpoint_url":"http://localhost:9090/hook"}'
```

Returns `201` with an `id` (`sub_…`) — copy it for the next step.

Alternatively, seed at startup instead of via HTTP:

```bash
# seed.json
# [{"customer_id":"acme","endpoint_url":"http://localhost:9090/hook"}]
SEED_FILE=seed.json go run ./cmd/server
```

Seeding is idempotent: entries that already exist are skipped.

## 4. Send an event

```bash
curl -s -X POST localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"subscription_id":"<SUB_ID>","payload":{"hello":"world"}}'
```

→ `202 Accepted`

```json
{"event_id":"evt_...","status":"pending"}
```

The event is persisted **before** the 202 is written; delivery is asynchronous.

Validation happens before anything is stored (FR2), so bad input leaves no rows:

- unknown `subscription_id` → `404`
- missing/invalid `payload` → `400`
- payload over 1 MiB (`MAX_PAYLOAD_BYTES`) → `413`

## 5. Watch the delivery

Within ~250ms (poll interval) Terminal A logs:

```
received delivery event_id=evt_... attempt=1 path=/hook duration=1.2ms
```

Then inspect the event:

```bash
curl -s localhost:8080/events/evt_... | jq
```

```json
{
  "event_id": "evt_...",
  "status": "delivered",
  "attempt_count": 1,
  "sequence": 1,
  "next_attempt_at": "...",
  "delivered_at": "...",
  "attempts": [
    {"attempt": 1, "attempted_at": "...", "http_status": 200, "duration_ms": 1}
  ]
}
```

List / filter:

```bash
curl -s 'localhost:8080/events?status=delivered&limit=10'
curl -s 'localhost:8080/events?customer_id=acme'
curl -s localhost:8080/subscriptions
```

**Outgoing request shape** — every delivery POSTs:

- Headers: `X-Event-Id`, `X-Delivery-Attempt`, `Content-Type: application/json`
- Body: `{"event_id": ..., "created_at": ..., "sequence": ..., "payload": ...}`

## 6. Failure scenarios to try

Restart Terminal B's mock with a new mode, send a fresh event, then watch
`GET /events/{id}`.

| Scenario | What you'll see |
|---|---|
| `-mode fail` (500s) | Retries with exponential backoff: attempt 1 now, attempt 2 ~10s later, then ~20s, ~40s… (`next_attempt_at` counts down) |
| `-mode flaky -fail-count 2` | First 2 attempts fail, the 3rd succeeds → `delivered` |
| `-mode slow -delay 15s` | Worker gives up at the 10s request timeout, records the timeout, releases the slot; retries later |
| `-mode down -down-for 90s` | 503s; the circuit breaker opens after 5 consecutive failures with growing cool-down, then delivery resumes on recovery |
| Kill the mock entirely | Network errors → backoff; restart it and the backlog drains |

Speed the demos up so you're not waiting on real timers:

```bash
RETRY_INITIAL_DELAY=2s POLL_INTERVAL=100ms go run ./cmd/server
```

Retry classification: network errors, timeouts, `5xx`, `408` and `429`
(honouring `Retry-After`) are retried; any other `4xx` fails immediately with
the reason recorded. Events hit a `failed` terminal state after the 24h
deadline — rows and attempt history are never deleted.

## 7. Other things to poke at

- **Graceful shutdown:** Ctrl-C the server → `shutdown signal received` …
  `shutdown complete`. Anything still leased is retried after restart.
- **Persistence:** restart the server, `curl /healthz` — counts unchanged.
- **Crash safety:** kill the server mid-delivery; the expired lease makes the
  event claimable again after restart (duplicates are expected — `event_id` is
  the idempotency key).
- **Test suite:** `go test ./... -race` runs the scenario tests (happy path,
  outage with isolation, slow endpoint, crash/lease expiry) with times scaled
  down, so it's fast.

## 8. Configuration

All via environment variables, defaults in `internal/config/config.go`:

| Variable | Default |
|---|---|
| `ADDR` | `:8080` |
| `DB_PATH` | `webhook.db` |
| `SEED_FILE` | *(unset)* |
| `WORKERS` | `8` |
| `PER_CUSTOMER_CAP` | `2` |
| `POLL_INTERVAL` | `250ms` |
| `LEASE_DURATION` | `30s` (must be > `REQUEST_TIMEOUT`) |
| `REQUEST_TIMEOUT` | `10s` |
| `SHUTDOWN_TIMEOUT` | `15s` |
| `MIN_RETRY_GAP` | `1s` |
| `MAX_PAYLOAD_BYTES` | `1048576` (1 MiB) |
| `RETRY_INITIAL_DELAY` | `10s` |
| `RETRY_MULTIPLIER` | `2` |
| `RETRY_JITTER` | `0.2` (±20%) |
| `RETRY_MAX_DELAY` | `30m` |
| `RETRY_DEADLINE` | `24h` |
| `BREAKER_THRESHOLD` | `5` |
| `BREAKER_COOLDOWN` | `30s` |
| `BREAKER_MAX_COOLDOWN` | `10m` |

Invalid values are an error at startup rather than a silent fallback.

## 9. Reset

```bash
rm webhook.db   # deletes all events and subscriptions
```

## 10. API summary

| Method & path | Purpose |
|---|---|
| `POST /events` | Ingest an event → `202` |
| `GET /events/{id}` | Event status + attempt history |
| `GET /events` | List, filter by `status`, `customer_id`, `subscription_id`, `limit` |
| `POST /subscriptions` | Register an endpoint → `201` |
| `GET /subscriptions` | List subscriptions |
| `GET /healthz` | Liveness + pending/delivered/failed counters |

