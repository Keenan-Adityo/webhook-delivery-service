# DECISIONS.md

## 1. Delivery semantics: at-least-once

For delivery semantics I've chosen at-least-once to make sure of the events will not be lost. the trade-offs will be data duplication, and to mitigate this risk the data also given event_id as the idempotency key.

## 2. Retry Behavior : Retry with exponential backoff

Retry behavior uses an exponential backoff mechanism with jitter for its retries. When a request fails, it waits an initial 10 seconds before trying again. If failures continue, the delay time doubles and includes a randomized ±20% variation (jitter) to prevent a flood of simultaneous requests. This delay is capped at 30 minutes—with the jitter applied before the cap to ensure the wait time never exceeds this limit—and all retry attempts stop 24 hours after the event was created.

## 3. Long outages : Shared worker pool with bounded concurrency

The system relies on a shared pool of eight workers that process pending deliveries by grabbing the oldest ones first. If a delivery fails, it is simply rescheduled with a future retry time (next_attempt_at). This ensures that no worker ever gets stuck waiting on an unresponsive endpoint, meaning even a six-hour outage consumes zero worker resources between attempts. To prevent a single slow customer from monopolizing the system, the database query strictly limits each customer to a maximum of two active tasks at a time, ensuring fair access to the worker pool.

## 4. Ordering: no ordering guarantee

Under normal conditions, picking the oldest pending events first results in a roughly chronological delivery. However, because the system processes events concurrently and reschedules failed ones, the final delivery order will inevitably become mixed up. To solve this, every payload includes an event_id, a created_at timestamp, and a per-customer sequence number. This empowers the receiving end (the consumer) to safely deduplicate and reorder the events themselves.


## What to do if have more time
*   **Adding more test** 

Given more time and if need to support larger scale of user, I would prioritize the following architectural changes:

*   **Horizontal Scaling & Database Migration:** The current SQLite implementation restricts the service to a single process. To handle massive traffic, I would migrate the database to PostgreSQL. This would allow the system to scale horizontally while using distributed locks for worker tasks. 
*   **Dedicated Tenant Isolation:** The shared worker pool was chosen for rapid development and simplicity. At a larger scale, I would implement dedicated queues and worker pools for high-volume enterprise customers. 
