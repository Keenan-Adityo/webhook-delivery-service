# AGENTS.md

Rules for any AI coding agent working in this repository. Read this fully before making changes. Also read `PRD_Webhook_Delivery_Service_fix.pdf` (the source of truth for scope and design) and `DECISIONS.md`.

## 1. Project context

- **What:** An internal Go service that accepts events via `POST /events` and reliably delivers them over HTTP POST to customer-registered endpoints, with retries, status querying, and customer isolation.
- **Why it exists:** This is a time-boxed take-home test (~3 hours). The goal is a **small, well-reasoned solution with clear trade-offs**, not an exhaustive one. The owner must be able to explain and defend every decision in an interview.
- **Owner:** The human developer. They direct; you implement. Never make a design decision silently.

## 2. Hard rules (never violate)

1. **Git is off-limits.** Do NOT run `git commit`, `git push`, `git add`, `git tag`, `git merge`, `git rebase`, `git reset`, `git checkout`, `git stash`, or any command that changes repository history, branches, or remotes. The owner handles all version control. Read-only commands (`git status`, `git diff`, `git log`) are allowed.
2. **No GitHub/remote interaction.** No `gh` CLI, no creating PRs/issues, no pushing, no changing remotes.
3. **Stay inside scope.** Do not build anything listed under non-goals (section 3). If you think something out of scope is needed, stop and ask.
4. **No destructive actions without asking.** Do not delete files or directories, drop databases, or run `rm -rf`. Ask first.
5. **No secrets or network calls to real services.** Customer endpoints are always mocked. Do not add telemetry or call external APIs.
6. **No new dependencies without approval.** Prefer the Go standard library. If a dependency is truly needed, explain why and wait for confirmation. Pre-approved: a pure-Go SQLite driver (e.g. `modernc.org/sqlite`).
7. **Do not silently change the PRD's design decisions.** If code and PRD conflict, flag it and ask which wins.

## 3. Scope

### In scope
- HTTP API: `POST /events`, `GET /events/{id}`, `GET /events` (stretch), subscription registration (endpoint or seed config), `/healthz`.
- SQLite datastore (source of truth), transactional claims with leases.
- Dispatcher + fixed worker pool delivering via HTTP POST with per-request timeout.
- Retry with exponential backoff + jitter, max delay, and overall deadline; terminal `failed` state.
- Per-customer concurrency cap and per-endpoint circuit breaker.
- Graceful shutdown, structured logs, simple counters.
- Mock customer endpoint tool and tests.
- `DECISIONS.md`.

### Out of scope (do NOT build)
- Authentication / login, multi-tenant billing, production deployment (Docker/K8s/CI/CD).
- **Any UI.** No React frontend. A status API is enough.
- Multi-instance / distributed operation (single process only).
- Per-customer queues, strict ordering mode, replay UI. These are "next steps" to mention in `DECISIONS.md`, not to implement.

## 4. Design decisions (locked, per PRD)

| Topic | Decision |
|---|---|
| Delivery semantics | **At-least-once.** Persist before returning 202. Duplicates possible; `event_id` is the idempotency key. |
| Retry | Exponential backoff, multiplier 2x, initial 10s, jitter ±20%, max delay 30 min, deadline 24h, per-request timeout 10s. All configurable. |
| Retry classification | Retry on network error, timeout, 5xx, 408, 429 (respect `Retry-After`). Other 4xx → fail early with reason recorded. |
| Terminal state | `failed` after deadline (or permanent 4xx). Never deleted; full attempt history kept. |
| Long outages | Shared worker pool (default 8) with bounded concurrency. Failed event = row with future `next_attempt_at`; workers are never parked on a dead endpoint. Per-customer cap (default 2 in-flight). Circuit breaker after 5 consecutive failures with growing cool-down. |
| Ordering | No strict guarantee. Oldest-due-first; deliveries carry `event_id`, `created_at`, and per-customer `sequence`. |
| Crash safety | Claim sets `lease_until`. Expired lease → event is picked up again. |
| Datastore | SQLite (pure Go), single process, one-line run. |

## 5. Tech stack and layout

- **Language:** Go (latest stable the owner uses; check `go.mod`).
- **Run command (must keep working):** `go run ./cmd/server`
- **Suggested layout** (confirm before deviating):

```
cmd/server/          # main entrypoint
cmd/mockendpoint/    # mock customer endpoint tool
internal/api/        # HTTP handlers, validation
internal/store/      # SQLite, migrations, queries
internal/dispatcher/ # scheduler, worker pool, claims, leases
internal/delivery/   # HTTP sender, attempt recording
internal/retry/      # backoff calculation, classification
internal/breaker/    # circuit breaker
internal/config/     # config with defaults
DECISIONS.md
AGENTS.md
```

## 6. Coding standards

- Idiomatic Go. Run `gofmt` and `go vet` before reporting work as done.
- Keep it **small and readable**. Prefer simple code over clever abstractions. No premature interfaces, no unused layers.
- Pass `context.Context` as the first parameter for anything that does I/O or can block. Respect cancellation.
- Wrap errors with context (`fmt.Errorf("claim event: %w", err)`). Never swallow errors.
- No global mutable state. Inject dependencies (store, clock, config).
- **Inject a clock** (`func() time.Time` or small interface) so backoff, leases, and deadlines are testable without sleeping.
- No goroutine leaks: every goroutine must have a clear stop condition tied to context/shutdown.
- All time-based parameters come from config with the PRD defaults, so tests can scale them down.
- Use parameterized SQL only. Never build queries with string concatenation of user input.
- Comments explain **why**, not what. Add a short comment wherever a trade-off or non-obvious decision lives (leases, claim query, breaker, jitter).

## 7. Correctness rules specific to this system

- An event must be **persisted before** the 202 response is sent.
- Claiming a due event must be **atomic** (single transaction) so two workers never take the same row, and it must set `lease_until`.
- A worker must never block waiting for a dead endpoint between attempts; schedule via `next_attempt_at`.
- Always record a `delivery_attempts` row for every attempt (status, error, duration).
- Payload size limit and request body validation happen **before** storing; invalid input stores nothing (FR2).
- Never delete events. Terminal `failed` rows are retained.
- Graceful shutdown: stop claiming, let in-flight requests finish within a timeout, then exit.
- Outgoing request must include headers `X-Event-Id`, `X-Delivery-Attempt`, `Content-Type: application/json`, and body `{event_id, created_at, sequence, payload}`.

## 8. Testing rules

- Write tests alongside the code they cover; do not leave testing to the end.
- **Required unit tests:** backoff calculation (bounds, jitter range, cap) and retry classification (which statuses retry).
- **Required scenarios** (via the mock endpoint, with times scaled down by config): happy path; endpoint down then recovers; long outage with isolation (other customers stay timely, breaker opens, stuck events end `failed` after deadline); slow endpoint (workers released at timeout, per-customer cap holds); crash mid-delivery (lease expires, retried, duplicate acceptable).
- Tests must be **deterministic and fast**. No real sleeps for long durations; use the injected clock or scaled config.
- Do not chase coverage percentage. Test the behaviors above.
- Run `go test ./...` (and `-race` where practical) before saying a task is done.

## 9. Workflow with the owner

- **Work in small steps** following the PRD plan: (1) setup/schema/subscriptions, (2) ingest + status API, (3) dispatcher/workers/retry, (4) per-customer cap + breaker, (5) mock endpoint + tests, (6) `DECISIONS.md` + wrap-up. Finish and report one step before starting the next.
- **Explain as you go.** The owner must understand and defend the code in an interview. After each step, give a short summary: what changed, why, and what trade-off it involves.
- **Ask before deciding.** If a requirement is ambiguous or a choice has real trade-offs, present 2-3 options briefly with your recommendation instead of picking silently.
- **Be honest about limits.** If something is incomplete, untested, or you are unsure, say so. Never claim tests pass without running them.
- **Respect the time cap (~3 hours).** Prefer the smaller solution. If something is running long, stop and propose what to cut and note it as a "next step."
- **Do not over-engineer.** If you catch yourself adding abstractions, configuration, or features the PRD does not require, stop and remove them.

## 10. AI transcript and deliverables

- The test requires the **complete, unedited transcript** of AI conversations to be submitted. Do not suggest editing, summarizing, or removing parts of it.
- Deliverables: code with a one-line run command, a short `DECISIONS.md` (half a page, covering delivery semantics, retry, long outages, ordering, and what was deliberately left out), the AI transcript, and an optional workflow reflection.
- When asked to draft `DECISIONS.md`, keep it concise and make sure it reflects what was **actually implemented**, not aspirations.

## 11. Definition of done (per task)

- [ ] Code compiles; `gofmt` and `go vet` are clean.
- [ ] Relevant tests written and passing (`go test ./...`).
- [ ] Matches the PRD and locked decisions, or deviations were flagged to the owner.
- [ ] No out-of-scope features added; no new dependencies without approval.
- [ ] No git commands that modify state were run.
- [ ] Short summary given to the owner: what, why, trade-offs, what is left.
