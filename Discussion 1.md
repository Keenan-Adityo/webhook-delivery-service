  
Junior SE Test V2.pdfPDF  
let's discuss about the problem, what u recommend and why  
I've read your technical test brief. My recommendation is to build a small, reliable webhook delivery service in Go, focusing on the decisions that make delivery reliable rather than building lots of features.

The brief explicitly says the company cares about engineering judgment, trade-offs, and how you use AI—not typing speed or framework choice. You have about three hours, and you'll need to explain your decisions and potentially extend the implementation live in a later interview.  
Junior SE Test V2.pdf

Let's discuss the problem and the design before writing the PRD or asking an AI agent to implement anything.

# 1\. First, understand the actual problem

Imagine you're building a service for an e-commerce platform. Whenever an order is paid, your system needs to notify another company's server.  
Internal Producer  
An order is paid

Submit event  
Webhook Service  
Accept, store, schedule, retry

HTTP POST  
Customer's Endpoint  
Receives the order-paid event

The challenge is not making an HTTP POST. That's easy. The challenge is what happens when something goes wrong.

For example:

1. Your service accepts an order.paid event.  
2. It tries to send the event to the customer's endpoint.  
3. The endpoint is down.  
4. Your service must remember that the event still needs delivery.  
5. It retries later, without losing the event or blocking deliveries to other customers.  
6. It records the final outcome so someone can inspect the event's status.

The brief specifically asks you to make decisions about delivery guarantees, retries, long outages, and ordering. Those four decisions should drive your architecture.  
Junior SE Test V2.pdf

# 2\. My recommended design

I'd choose the following architecture for this test.  
Go \+ standard net/http  
Expose endpoints to register customer subscriptions, accept events, and inspect delivery status. No need for a large web framework.  
SQLite  
Persist subscriptions, event payloads, delivery attempts, statuses, and retry schedules. This is safer than in-memory storage when the process restarts.  
Background worker  
Find due deliveries, send HTTP requests with timeouts, record results, and schedule retries. The API doesn't have to wait for customer endpoints.  
Simple separation of responsibilities  
HTTP handlers → service/business logic → repository/database, plus a separate delivery worker. Keep the code easy to test and review.

Why this architecture? It directly solves the assignment without introducing Redis, Kafka, Kubernetes, microservices, or other infrastructure that would consume your three-hour budget.

## 3\. The four decisions I'd make

These are the most important parts of the exercise.

### Decision 1 — Delivery semantics

Recommendation: at-least-once delivery.

This means the service keeps retrying until it receives a successful response or reaches its retry deadline. A successful HTTP response, such as 2xx, counts as delivery.

Why not the alternatives?

| Guarantee | My assessment |
| ----- | ----- |
| At-most-once | Simpler, but an event can be lost if a delivery fails and isn't retried. |
| At-least-once | A good balance of reliability and implementation complexity. |
| Exactly-once | Not realistic to guarantee for arbitrary HTTP endpoints across network failures without additional cooperation and assumptions. |

There's an important edge case: the customer's server might process the event successfully, but the response gets lost. Your service sees a timeout and retries, so the customer might receive the same event twice.

I'd give each event a stable ID and send it as an Idempotency-Key header. The customer can use that ID to deduplicate processing.

The trade-off: we accept possible duplicate delivery in exchange for reducing the risk of lost events. We also make the limitation clear to customers.

### Decision 2 — Retry behavior

Recommendation: exponential backoff with a maximum retry window of 24 hours.

For example:

| Attempt | Example delay after failure |
| ----- | ----- |
| 1 | 1 second |
| 2 | 5 seconds |
| 3 | 30 seconds |
| 4 | 2 minutes |
| 5 | 10 minutes |
| Later attempts | Increasing delays, capped at 1 hour |

These are proposed values, not requirements from the brief. I'd keep them configurable.

I'd classify outcomes as follows:

* 2xx: mark as delivered.  
* 5xx, network errors, and timeouts: retry.  
* 429: respect Retry-After when valid, otherwise use backoff.  
* Other 4xx: generally treat as permanent failures, except for explicitly retryable cases.

Once the 24-hour retry window expires, mark the delivery failed and retain the record and its attempt history. That makes the outcome visible and allows a future manual replay feature.

One nuance: a failed event isn't the same as a deleted event. We can stop automatic retries without losing the event's data.

### Decision 3 — What happens during a six-hour outage?

Recommendation: persist each delivery independently and process it in the background.

Imagine Customer A's endpoint is unavailable for six hours, while Customer B's endpoint is working normally.

With a poorly designed synchronous system, Customer A's slow endpoint could delay requests or occupy all available processing capacity.

Instead, I'd use this flow:

1. Accept and persist the event.  
2. Return an event ID to the producer.  
3. Let a background worker process due deliveries.  
4. If Customer A fails, record the attempt and schedule the next retry.  
5. Continue processing other due deliveries, including Customer B's.  
6. Resume Customer A's deliveries when their endpoint becomes available.

For this small project, a database-backed retry schedule is enough. You don't need a separate message broker.

I'd also set an HTTP timeout and limit worker concurrency so an endpoint that never responds cannot consume resources indefinitely.

### Decision 4 — Event ordering

Recommendation: don't guarantee strict delivery ordering in the initial version.

Suppose a customer receives these events:

1. order.created  
2. order.paid  
3. order.shipped

If order.created fails and is scheduled for retry, the later events might succeed first. That can cause problems if the receiving system expects events in order.

There are two reasonable approaches:

* Independent delivery: simpler, allows more concurrency, but events may arrive out of order.  
* Per-customer ordered delivery: later events wait until earlier events are successfully delivered, but a single failing event can block that customer's queue.

For a three-hour test, I'd start with independent delivery, document the limitation, and preserve event creation timestamps and IDs. If strict ordering is a business requirement, I'd extend the design with per-customer sequencing.

Importantly, I'd keep Customer A's retries from blocking Customer B, regardless of the ordering policy.  
Junior SE Test V2.pdf

## 4\. What I'd actually implement within three hours

I would keep the scope deliberately small.

## Minimum viable submission

Implementation checklist  
0 of 6  
Subscription management  
Register a customer ID and destination URL.  
Event ingestion  
Accept JSON, validate the subscription, persist the event and delivery, and return an ID.  
Delivery worker  
Send HTTP POST requests with a timeout and stable event ID.  
Retry mechanism  
Persist attempt count and next\_attempt\_at; retry transient failures.  
Delivery status  
Retrieve an event's delivery state and attempt history.  
Tests and documentation  
Test success, retry, timeout, and eventual failure; provide run instructions and DECISIONS.md.

I'd avoid building a UI, authentication, a distributed queue, elaborate observability, or a deployment pipeline because the brief explicitly excludes or deprioritizes these.  
Junior SE Test V2.pdf

For the database, I'd start with three tables:

* subscriptions: customer ID and destination URL.  
* events: event ID, customer ID, payload, creation time.  
* deliveries: event ID, status, attempt count, next attempt time, last error, and delivery timestamps.

This keeps event data separate from delivery state and gives us room to extend the system later.

## 5\. One thing I'd do differently from simply telling an AI agent to build it

Since this assessment explicitly evaluates your reasoning, I'd make the design decisions first, then ask the agent to implement a bounded scope.

For example, you should be able to explain why the service persists events before attempting delivery, why retries happen asynchronously, and why the system offers at-least-once rather than exactly-once delivery.

Also, test the failure cases yourself. A happy-path demo alone doesn't establish that a webhook service is reliable.

The deliverables should include the code, a one-line run command, DECISIONS.md, and the complete, unedited AI conversation transcript, as requested by the brief.  
Junior SE Test V2.pdf

## Let's discuss one architectural decision first

Before we write the PRD, I want to hear your reasoning rather than just choose everything for you.

When a producer sends an event, should the API wait for the customer's endpoint to respond, or should it save the event and return immediately?  
A. Wait for delivery  
The API sends the webhook and responds to the producer only after the customer's endpoint responds.  
B. Persist and return immediately  
The API saves the event and schedules delivery in the background.  
What's your main reason?  
Simpler implementation  
Reliability and avoiding lost events  
Response time and scalability

My initial preference is B, because it separates accepting an event from delivering it. But I want you to understand why that separation matters before we move to the next decision.

For the webhook delivery service technical test, I choose B: persist the event and return immediately, delivering asynchronously. My main consideration is reliability and avoiding lost events. Discuss the trade-offs with me.

Good choice. I'd also choose B: persist the event and return immediately for this technical test. Your reasoning about reliability is on the right track, but there's an important distinction to understand: returning immediately doesn't automatically make a system reliable. Persisting the event safely before acknowledging it is what makes this approach reliable.

Let's examine the trade-offs as if we were discussing your design in the technical interview.

## 1\. Compare the two approaches

A. Synchronous delivery

Producer sends event

API sends HTTP POST to customer

Wait for response or timeout

Return API response

Advantage: straightforward flow. Disadvantages: slow responses, coupling to customer availability, and complicated failure handling.

B. Asynchronous delivery (your choice)

Producer sends event

Validate and persist event

Return 202 Accepted \+ event ID

Producer's request is complete

Background worker picks up event

POST to customer endpoint

Record result or schedule retry

Advantage: producer response isn't tied to customer response time. Disadvantage: more moving parts and a need to manage persistent work.

## 2\. What happens if something fails?

This is where your decision becomes interesting.

| Failure scenario | Expected behavior |
| ----- | ----- |
| Customer endpoint is down | Keep the event; schedule a retry. |
| Customer endpoint takes 20 seconds to respond | The producer doesn't need to wait 20 seconds. |
| Service restarts after accepting an event | Recover pending deliveries from the database after restart. |
| Database write fails | Return an error; don't acknowledge the event as accepted. |
| Worker crashes during delivery | The unfinished delivery must become eligible for processing again. |
| Customer processes the event but the response is lost | Retry may produce a duplicate; use a stable event ID for deduplication. |

Notice the fourth scenario. Imagine your API returns 202 Accepted before saving the event, then the process crashes. The producer believes the event was accepted, but the event is gone.

That is exactly what we must avoid.

### The critical rule

Persist first, acknowledge second.

The sequence should be:

1. Validate the request and subscription.  
2. Store the event and its pending delivery record in a database transaction.  
3. Commit the transaction successfully.  
4. Return 202 Accepted with the event ID.  
5. Let the background worker handle delivery.

For the initial implementation, SQLite is sufficient. Use durable storage and configure SQLite appropriately, including its journaling and synchronization settings, rather than assuming every write is automatically safe against every kind of failure.

202 Accepted means the service has accepted the work, not that the customer's endpoint has received the event.

## 3\. Does asynchronous delivery guarantee that no events are lost?

No—not absolutely. This is a useful distinction to explain in your interview.

Consider these two separate guarantees:

* Acceptance durability: once the service acknowledges an event, the event is persisted so it can be recovered after a normal process restart.  
* Delivery guarantee: the service attempts delivery until it succeeds or reaches its configured terminal failure condition.

Your proposed architecture improves both, but each has its own failure modes.

For example, a server's disk could fail, the database could become corrupted, or the event could exhaust its retry window. A single SQLite database does not protect against every infrastructure failure.

For this assignment, I wouldn't add replication or distributed infrastructure. I'd state the guarantee precisely: accepted events are durably stored locally, retried on transient failures, and retained with a final status if delivery ultimately fails.

That is a much more defensible engineering claim than promising that events can never be lost.

## 4\. How does the background worker know what to process?

I'd keep it simple. The worker periodically queries the database for deliveries that are pending and due.

For example, a delivery record might look like this:

* event\_id: evt\_123  
* status: pending  
* attempt\_count: 2  
* next\_attempt\_at: 10:15:30

last\_error: connection timeout

The worker selects records whose next\_attempt\_at is in the past, claims them for processing, and attempts delivery.

After a successful POST, it records delivered. If delivery fails temporarily, it increments the attempt count and schedules another attempt.

One detail matters: don't hold a database transaction open while waiting for an HTTP response. Claim the work in a short transaction, release the database, perform the network request, and then persist the result.

For a single-process test, you can use a worker goroutine and SQLite. However, you still need to consider what happens if the process crashes after claiming work. A recoverable claim or lease is one way to make unfinished deliveries available again.

## 5\. What trade-offs should you defend?

I'd prepare these four points for the interview.

Fast acknowledgement versus confirmed delivery

The producer gets a quick response, but must check delivery status separately. This is a good trade-off when producers shouldn't be coupled to customer availability.

Durability versus infrastructure complexity

SQLite provides persistence without operating a separate queue service. It is appropriate for a single-process prototype, but has scaling and availability limits.

Retries versus duplicate side effects

Retries improve delivery reliability but can cause duplicate processing. Stable event IDs help customers implement idempotency.

Background processing versus operational complexity

Workers decouple delivery from ingestion, but introduce scheduling, recovery, concurrency, and status-management responsibilities.

## 6\. Let's test your reasoning

Imagine the following situation:

Your API saves an event to SQLite, commits the transaction, and returns 202 Accepted. A background worker sends the event to the customer. The customer's server processes the event successfully, but your service times out before receiving the response.

What should your worker do?

A. Mark it as delivered because the customer probably processed it.

B. Retry the delivery because the service cannot confirm success.

C. Mark it as permanently failed because the request timed out.

This scenario is especially important because it reveals why reliable delivery and exactly-once processing are not the same thing. Once you're comfortable explaining it, we'll have a strong foundation for deciding how to design the worker and database.

My answer to the webhook timeout scenario is B: retry because the service cannot confirm success. Explain whether my reasoning is correct, including the consequences for at-least-once delivery and duplicate processing.

Your answer is correct: B — retry because the service cannot confirm success.

You've understood the central principle behind at-least-once delivery: a timeout does not necessarily mean the customer failed to process the event. It means your service doesn't know what happened.

Let's explore the consequences, because this is exactly the kind of reasoning you'll need during your technical interview.

## 1\. What actually happened?

Imagine your webhook service sends an order.paid event to a customer's server.

Webhook service

Sends event evt\_123

Customer's server

Event processed

Order marked as paid

Response lost or delayed

Webhook service

Timeout

Cannot confirm whether processing succeeded

Your service cannot distinguish between these two situations:

* Scenario A: The customer never received the request.  
* Scenario B: The customer processed the request, but the response never reached your service.

From the sender's perspective, both can look like a timeout.

If you mark the event as delivered, Scenario A could cause a lost event. If you retry, Scenario B could cause duplicate processing.

For an at-least-once system, we choose to retry.

## 2\. What does at-least-once delivery actually guarantee?

At-least-once delivery means the service keeps attempting delivery until it receives a successful response or reaches its configured terminal condition.

It does not guarantee that the customer processes every event exactly once.

| Strategy | Risk |
| ----- | ----- |
| Never retry after a timeout | Events may never reach the customer. |
| Retry after a timeout | The customer may process an event more than once. |
| Retry with idempotency support | The customer can recognize duplicate events and avoid repeating the same operation. |

One important qualification: if your service has a finite retry window, it cannot guarantee eventual delivery when an endpoint remains unavailable indefinitely. The precise guarantee is that it retries according to policy and retains a final failure record when that policy is exhausted.

That distinction will make your explanation much stronger in an interview.

## 3\. How do we prevent duplicate processing?

The solution is idempotency.

In simple terms, an operation is idempotent if repeating it produces the same final effect as performing it once.

For example, consider two ways of handling an order.paid event.

Unsafe: apply the operation every time

balance \+= 100

If the same event is processed twice, the balance increases by 200 instead of 100\.

Safer: identify the event before applying it

processOnce(event.id)

The customer records the event ID and avoids applying the same business operation twice.

For your implementation, I recommend giving every event a stable ID, such as evt\_123, and sending it on every attempt:

* POST /webhooks  
* Content-Type: application/json

Idempotency-Key: evt\_123

The same event must keep the same ID across all retries. Generating a new ID for each attempt defeats the purpose.

Your service can support this convention, but the receiving customer must implement deduplication for it to prevent duplicate side effects. A header alone doesn't guarantee idempotency.

For robust customer-side processing, the receiver should persist the event ID and its business operation atomically, so simultaneous retries or crashes cannot cause the same operation to be applied twice.

## 4\. How should your worker handle this timeout?

I'd implement the following behavior:

1\. Send the event

POST the payload with the stable event ID and a configured HTTP timeout.

2\. Timeout occurs

Record the failed attempt and its error. Do not mark the event as delivered.

3\. Schedule a retry

Increment the attempt count and calculate next\_attempt\_at using exponential backoff.

4\. Record the next outcome

If the next attempt returns 2xx, mark the delivery as delivered. If attempts are exhausted, mark it failed.

There's another subtlety: your system should define which HTTP responses count as success, and record every attempt. If a 2xx response arrives, it is reasonable to treat the delivery as successful, even though HTTP alone cannot prove that the customer's business logic was correct.

## 5\. One more engineering detail: duplicate attempts inside your own service

Suppose your worker sends the event successfully, but the process crashes before updating SQLite to delivered.

After restarting, the worker sees the event as pending and sends it again.

This is another source of duplicates, even if the customer's endpoint behaved perfectly.

That's why the database state and network request cannot be treated as one atomic transaction: SQLite cannot atomically commit a change together with an arbitrary remote HTTP operation.

For this test, I'd accept that limitation, keep the event ID stable, persist delivery attempts, and make pending work recoverable after a restart. I would not try to claim exactly-once delivery.

## Next design decision: how should retries work?

You understand why retries are necessary. Now let's consider the policy.

Suppose a customer's endpoint is down for six hours. Which retry strategy would you choose?

A. Retry immediately

Attempt delivery again as soon as a request fails.

B. Fixed interval

Retry every 30 seconds, for example, until the retry limit is reached.

C. Exponential backoff

Increase the delay after consecutive failures, with a maximum delay and retry deadline.

My recommendation is C, because repeated requests to an unavailable endpoint can make an outage worse. But the interesting part is how you balance recovery speed, server load, and the retry deadline.

For my webhook delivery technical test, I choose retry strategy C: exponential backoff with a maximum delay and retry deadline. Explain the trade-offs, and help me choose sensible retry limits for a three-hour Go and SQLite implementation.

Your choice is good. For this webhook service, I'd use exponential backoff with a maximum delay and a retry deadline, exactly as you suggested.

The key trade-off is that you want to recover quickly when an endpoint briefly fails, but avoid hammering an endpoint that's been down for hours.

For your three-hour Go \+ SQLite technical test, I'd prioritize a retry policy that's reliable, easy to explain, and simple to test—not a production-grade scheduling system.

## 1\. Why exponential backoff?

Compare the three strategies:

Immediate retry

Every failure triggers another request immediately.

Problem: can flood an unhealthy endpoint with requests and waste worker capacity.

Fixed interval

Retry every 30 seconds, for example.

Simple and predictable, but many failed deliveries can repeatedly hit the endpoint at the same time.

Exponential backoff

Increase the delay after each consecutive failure, up to a maximum.

Reduces unnecessary traffic during long outages while still retrying relatively quickly after brief failures.

The basic formula is:

dn=min⁡(d0×2n−1,dmax⁡)

*d*

*n*

​

\=min(*d*

0

​

×2

*n*−1

,*d*

max

​

)

Where:

* dn  
* *d*  
* *n*  
* ​  
* is the delay before the next attempt.  
* d0  
* *d*  
* 0  
* ​  
* is the initial delay.  
* n  
* *n* is the number of the retry following a failure.  
* dmax⁡  
* *d*  
* max  
* ​  
* is the maximum delay.

For example, with a 1-second initial delay and a 5-minute maximum, the delays grow approximately like this:

| Retry | Delay before next attempt |
| ----- | ----- |
| 1 | 1 second |
| 2 | 2 seconds |
| 3 | 4 seconds |
| 4 | 8 seconds |
| 5 | 16 seconds |
| 6 | 32 seconds |
| 7 | 1 minute 4 seconds |
| 8 | 2 minutes 8 seconds |
| 9 | 4 minutes 16 seconds |
| 10+ | 5 minutes, capped |

These are illustrative values; the actual schedule should be configurable.

## 2\. My recommended retry policy

For this assignment, I'd start with these settings:

Initial delay

### 1 second

Backoff multiplier

### 2×

Maximum delay

### 1 hour

Retry deadline

### 24 hours

HTTP request timeout

### 5 seconds

Suggested starting values, not requirements from the assessment. Make them configurable so you can tune them during testing.

Why these values?

* 1 second initial delay: a brief network issue can recover quickly.  
* 1-hour maximum delay: repeated failures eventually become less frequent, reducing pressure on a struggling endpoint.  
* 24-hour retry deadline: gives the customer time to recover from an extended outage.  
* 5-second request timeout: prevents a slow endpoint from occupying a worker indefinitely.

I'd also consider adding *jitter*, a small random variation to retry delays. Without it, thousands of deliveries that fail together can all retry together. For a three-hour assignment, jitter is a useful enhancement, but I'd implement and test the basic backoff first.

One important distinction: the 24-hour deadline should be calculated from the time the delivery first became eligible for delivery, not reset every time it fails. Otherwise, a delivery could retry forever.

## 3\. What happens after 24 hours?

I recommend marking the delivery failed, while retaining the event and its attempt history.

The state flow would be:

pending

Waiting for delivery or retry

delivering

Worker is attempting HTTP POST

delivered

HTTP 2xx

failed

Permanent error or exhausted policy

delivering is optional, but useful for visibility. If you implement it, make sure a crashed worker cannot leave deliveries stuck in that state indefinitely. A claim timeout or lease can make them eligible for recovery.

Also, failed shouldn't mean the event is deleted. A future manual retry or replay feature could be added, but I wouldn't spend time building it unless the core requirements are complete.

## 4\. How I'd implement this in Go

The main design principle is to persist the next retry time rather than sleeping inside the worker.

For example, store the following fields in the deliveries table:

| Column | Purpose |
| ----- | ----- |
| status | Current delivery state |
| attempt\_count | Number of delivery attempts |
| next\_attempt\_at | When the worker may try again |
| first\_attempt\_at | Start of the retry window |
| last\_error | Most recent failure reason |
| delivered\_at | Successful delivery timestamp, if any |

When a delivery fails:

1. Increment attempt\_count.  
2. Calculate the next delay.  
3. Check whether the retry deadline has passed.  
4. If the deadline has expired, mark the delivery failed.  
5. Otherwise, persist the new next\_attempt\_at and return control to the worker loop.

The worker can then query SQLite for pending deliveries where next\_attempt\_at \<= now.

Why not use time.Sleep() for every retry? Because sleeping ties up a worker and loses the schedule when the process restarts unless you reconstruct it. A persisted schedule allows the service to recover pending work after a restart.

For a single-process service, a simple polling goroutine is sufficient. I'd avoid introducing a queue library or external message broker for this assessment.

## 5\. What I'd test

You don't need to wait 24 hours to prove that the policy works. Inject a clock or make retry settings configurable in tests so you can simulate elapsed time.

Prioritize these cases:

* A successful 2xx response marks the delivery as delivered.  
* A timeout schedules a retry with a later next\_attempt\_at.  
* Consecutive failures increase the delay until it reaches the cap.  
* A permanent HTTP error follows the chosen failure policy.  
* An expired retry deadline marks the delivery failed.  
* A service restart doesn't lose pending deliveries or their schedules.

These tests demonstrate that the design works, not merely that the happy path works.

## Next, let's decide how to isolate customer outages

You've now chosen two important policies:

* Persist events before acknowledging producers.  
* Retry transient delivery failures with exponential backoff.

The next question is about concurrency.

Suppose Customer A's endpoint is down for six hours and Customer B's endpoint is healthy. How should the worker handle their deliveries?

A. One sequential worker

Process one delivery at a time in a single loop. Simple, but a slow endpoint may delay other work.

B. Multiple workers with bounded concurrency

Allow several deliveries to run concurrently, with a limit to protect resources.

C. Separate processing per customer

Isolate each customer's delivery stream so one customer's outage is less likely to affect another.

My starting recommendation is B: bounded concurrency, with retry scheduling persisted in SQLite. It provides useful isolation without requiring a complicated per-customer queue system. The remaining question is how to avoid accidentally processing the same delivery concurrently.

### Chat paused until usage resets at 3:08 PM

You’ve reached the limit for chats that include files or images. Start a new text-only chat or upgrade to continue now.

* 

