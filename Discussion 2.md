

pdf

Let's discuss about the problem, what u recommend and why, I have done some research  for the architectural decision based on the problem given:

> 1. Deliver semantics: At-least-once, persist the event and return immediately, delivering asynchronously. My main consideration is reliability and avoiding lost events.  
> 2. Retry Behavior: retry with exponential backoff (Increase the delay after consecutive failures, with a maximum delay and retry deadline.)  
> 3. Long Outages:  
> 4. Ordering:

Long outages my choice might be Multiple workers with bounded concurrency or Separate processing per customer, Explain me each of it trade-offs

Read and updated memory

Your first two decisions are solid and easy to defend. Here’s how I’d think about the other two.

### Long outages: the two options

The brief’s real question is: *“What happens to everyone else’s deliveries meanwhile?”* The failure you must avoid is head-of-line blocking, where one dead customer starves healthy ones.

#### Option A: Shared worker pool with bounded concurrency

A fixed pool of N workers pulls due deliveries from the DB, ordered by next\_attempt\_at.

Pros

> * Simple. This is the smallest design that works, which fits the 3-hour budget.  
> * Predictable resource use: N workers means at most N in-flight HTTP calls and a bounded number of goroutines and sockets.  
> * Failed deliveries are just rows with a future next\_attempt\_at, so a down endpoint doesn’t hold a worker while it waits.

Cons

> * Noisy neighbor risk. If a customer’s endpoint is *slow* (not down) and hangs until the timeout, it can occupy many workers at once. A customer with a huge backlog can also fill the queue and delay others.  
> * No fairness by default. It’s first-come-first-served across all customers.  
> * Ordering is not preserved, because two workers can pick up events from the same customer simultaneously.

Mitigations (cheap)

> * A short per-request timeout (e.g. 5–10s).  
> * A per-customer concurrency cap (e.g. max 2 in-flight per customer), which gives you most of Option B’s isolation.  
> * A circuit breaker or backoff at the endpoint level. After several consecutive failures, stop trying that endpoint for a while instead of burning retries on each event.

#### Option B: Separate processing per customer

Each customer gets its own logical queue or goroutine.

Pros

> * Strong isolation. One customer’s outage can’t affect others.  
> * Natural ordering. One consumer per customer means sequential delivery, so ordering is easy to guarantee.  
> * Easy to reason about per-customer rate limiting and backoff.

Cons

> * Resource scaling. In Go, goroutines are cheap, but 10,000 customers means 10,000 long-lived loops, mostly idle. You need lazy spawning and idle teardown.  
> * More complex lifecycle: starting, stopping, restarting after a crash, and rebuilding state from the DB.  
> * Strict ordering has a cost. If event \#1 is failing, \#2 to \#100 wait behind it. That is head-of-line blocking *within* the customer. During a six-hour outage you build a large backlog that all floods out at once on recovery.  
> * Harder to run across multiple instances later, since you need to guarantee one consumer per customer (leases or locks).

### My recommendation

Use Option A with a per-customer concurrency cap. It is a hybrid, and it’s easy to explain:  
Shared bounded worker pool \+ per-customer concurrency limit \+ per-endpoint backoff/circuit breaker.

It gets you most of B’s isolation without B’s lifecycle complexity. For a single-process, 3-hour exercise, it’s the best simplicity-to-safety ratio. In DECISIONS.md, say that per-customer queues would be the next step if strict ordering became a requirement.

For the six-hour outage scenario, you can describe it concretely:

> 1. Attempts fail, so the event stays pending with a growing next\_attempt\_at.  
> 2. Backoff caps at some maximum (e.g. 30 min–1h), so you don’t hammer a dead endpoint.  
> 3. The circuit breaker pauses the endpoint, so workers stay free for other customers.  
> 4. After the retry deadline (e.g. 24–72h), the event goes to a terminal failed state. It is kept, not deleted, so it can be replayed manually.

### Ordering (your blank item)

You can defend either choice, but you have to state it. My suggestion:

> * No ordering guarantee. Document it, and include a monotonically increasing event\_id or created\_at plus an idempotency key in each webhook so the customer can sort and dedupe.  
> * This follows from your at-least-once choice: retries already mean duplicates and reordering can happen, so strict ordering would be inconsistent unless you also take on Option B’s cost.  
> * Real-world examples like Stripe take the same stance: they say ordering is not guaranteed and tell customers to design for it.

Mention the cost of this stance too: the customer’s handler must be idempotent and tolerate out-of-order events. That’s the “what it demands of the customer” part of the brief’s first question.

### Two quick flags

> 1. Your stack vs. the brief. Your memory notes mention React/TypeScript for the frontend, but the brief lists “no UI” as out of scope. A status endpoint like GET /events/{id} is enough. Spending time on a frontend could hurt you under the 3-hour cap.  
> 2. Backoff detail. Add jitter to your exponential backoff, otherwise many retries from a recovered outage fire at the same instant (thundering herd).

If you tell me which way you’re leaning on ordering, I can help you draft the DECISIONS.md section for all four decisions.

Let's do Long outages with option A: Shared worker pool with bounded concurrency.

Ordering. Do events for a given customer arrive in the order  
they were produced? Should they?  
what u recommend, why, and what the tradeoffs

Memori diperbarui webhook-delivery-test.md

Option A is a good pick, and it makes the ordering answer fairly clear.

### Do events arrive in order? Should they?

Recommendation: no strict ordering guarantee. Delivery is best-effort in roughly creation order, and each event carries metadata so the customer can reorder or dedupe.

#### Why this fits your design

> 1. It follows from your earlier choices. With at-least-once delivery, retries and a shared worker pool, reordering is inevitable. Event \#1 fails and retries in 30s while \#2 succeeds immediately, so the customer sees \#2 then \#1. Promising ordering on top of that would be a promise your architecture can’t keep.  
> 2. It avoids head-of-line blocking. Strict ordering means event \#2 can’t go out until \#1 is delivered or fails permanently. In a six-hour outage, every later event for that customer piles up behind one stuck event. That contradicts the goal of keeping the service resilient.  
> 3. It keeps the design simple. Guaranteeing order requires one consumer per customer (Option B), leases or locks, and a policy for what a “poison” event does to the queue. That’s a lot for a 3-hour exercise.

#### What you do to compensate

Include these in every webhook delivery:

> * event\_id: a unique ID, which is also the idempotency key for deduplication.  
> * created\_at and ideally a per-customer sequence number, so the customer can detect gaps and reorder.  
> * An attempt number, an optional header that helps debugging.

Also pick the oldest-first query order for workers (ORDER BY next\_attempt\_at, created\_at). Under normal conditions, events then arrive nearly in order. The ordering is a best-effort property, not a guarantee, and your DECISIONS.md should say so.

### Trade-offs

|  | No ordering guarantee (recommended) | Strict per-customer ordering |
| ----- | ----- | ----- |
| Complexity | Low | High (per-customer queues, locking) |
| Outage behavior | A stuck event delays only itself | A stuck event blocks that customer’s whole backlog |
| Recovery | Backlog drains in parallel | Drains serially, so it’s slower |
| Customer burden | Must handle out-of-order and duplicate events | Fewer surprises, but still must dedupe (at-least-once) |
| Fits Option A | Yes | No, it needs Option B-style design |

The main cost of my recommendation is on the customer: they must write idempotent handlers and cope with reordering. For events like “payment succeeded” followed by “refund issued”, out-of-order delivery can cause real bugs on their side. That’s why the sequence and created\_at fields matter, since they give the customer what they need to cope. You should name this cost explicitly in DECISIONS.md.

### A middle option you can mention

If you want to show depth without building it, note this as a “what I’d do next”: an optional per-subscription ordered mode where only subscriptions that opt in get serialized delivery (one in-flight event at a time), with a limit on how long a blocked event can hold the queue. Most customers keep the high-throughput default, and the few who need ordering accept the head-of-line blocking trade-off knowingly.

### Draft for DECISIONS.md

Ordering. No strict ordering guarantee. Workers pick the oldest due event first, so delivery is roughly chronological in the normal case, but retries and concurrent workers can reorder events. Each delivery includes event\_id, created\_at, and a per-customer sequence so consumers can dedupe and reorder. Trade-off: customers must handle out-of-order and duplicate deliveries. I chose this over strict ordering because enforcing it would require per-customer serialization, which lets one failing event block all later events for that customer during an outage. Next step: an opt-in ordered mode for subscriptions that need it.

If you’re happy with this, we’ve covered all four decisions and could move on to the data model and the worker loop next. Want to go there?

Buat Claude Doc dari jawaban terakhir Anda.

Make doc changes (batch)

Keputusan Ordering: Webhook Delivery Service  
Claude Docs·Only you   
“Keputusan Ordering: Webhook Delivery Service” sudah dibuat sebagai Claude Doc dan terbuka untuk Anda. Isinya rekomendasi tanpa jaminan ordering ketat, alasannya, tabel trade-off, opsi mode ordered opt-in, dan draft paragraf DECISIONS.md. Anda bisa mengedit langsung, memberi komentar, atau membagikannya.

Draft DECISIONS.md di dalamnya saya tulis dalam bahasa Inggris karena itu yang akan Anda kumpulkan, sedangkan catatan lainnya dalam bahasa Indonesia. Kalau mau, saya bisa lanjutkan ke data model dan worker loop, atau menyusun dokumen DECISIONS.md lengkap untuk keempat keputusan.

create PRD for GO project based on the PDF test project and the decision we discussed

Updated memory, used Claude Docs integration

PRD: Webhook Delivery Service (Go)  
Claude Docs·Only you   
The PRD is open for you as a Claude Doc: “PRD: Webhook Delivery Service (Go)”. It covers the goals, requirements, your four decisions, the architecture, the data model, the API, the test plan, a 3-hour time plan and the next steps.

I proposed some numbers and one choice that you should confirm or change:

> * Datastore: I suggested SQLite so the run command stays one line. Postgres is the more production-like option, but it needs Docker.  
> * Retry defaults: a 10s timeout, 10s initial delay, 2x multiplier, 30 min maximum delay and a 24h deadline. These are my suggestions, so adjust them to whatever you can defend in the interview.  
> * No React frontend: the brief puts a UI out of scope, so the PRD uses only a status API.

If you’d like, I can turn the PRD into a Go project skeleton next, starting with the folder layout, schema and the worker loop.


