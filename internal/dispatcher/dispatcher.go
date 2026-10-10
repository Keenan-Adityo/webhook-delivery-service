// Package dispatcher runs the fixed worker pool: claim a due event, deliver it,
// record the attempt, then mark delivered / schedule the retry / fail it.
package dispatcher

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"webhook-delivery-service/internal/breaker"
	"webhook-delivery-service/internal/config"
	"webhook-delivery-service/internal/delivery"
	"webhook-delivery-service/internal/retry"
	"webhook-delivery-service/internal/store"
)

type Dispatcher struct {
	cfg     config.Config
	store   *store.Store
	sender  *delivery.Sender
	breaker *breaker.Breaker
	log     *slog.Logger
	now     func() time.Time
	rnd     func() float64
}

// New wires the dispatcher. log, now and rnd may be nil (sensible defaults),
// which keeps tests small.
func New(
	cfg config.Config,
	st *store.Store,
	sender *delivery.Sender,
	brk *breaker.Breaker,
	log *slog.Logger,
	now func() time.Time,
	rnd func() float64,
) *Dispatcher {
	if log == nil {
		log = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	if rnd == nil {
		rnd = rand.Float64
	}
	return &Dispatcher{cfg: cfg, store: st, sender: sender, breaker: brk, log: log, now: now, rnd: rnd}
}

// Run starts the worker pool and blocks until ctx is cancelled and every
// worker has finished its current event (graceful shutdown: stop claiming,
// let in-flight work finish).
func (d *Dispatcher) Run(ctx context.Context) {
	d.log.Info("dispatcher started",
		"workers", d.cfg.Workers, "per_customer_cap", d.cfg.PerCustomerCap,
		"poll_interval", d.cfg.PollInterval, "request_timeout", d.cfg.RequestTimeout)

	var wg sync.WaitGroup
	for i := 0; i < d.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			d.worker(ctx, id)
		}(i)
	}
	wg.Wait()
	d.log.Info("dispatcher stopped")
}

func (d *Dispatcher) worker(ctx context.Context, id int) {
	for {
		// Checked before every claim so shutdown stops taking new work.
		if ctx.Err() != nil {
			return
		}
		ev, ok, err := d.store.ClaimDue(ctx, d.now(), d.now().Add(d.cfg.LeaseDuration), d.cfg.PerCustomerCap)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			d.log.Error("claim event", "worker", id, "error", err)
		}
		if !ok {
			if !sleep(ctx, d.cfg.PollInterval) {
				return
			}
			continue
		}
		// WithoutCancel: shutdown must not abort the bookkeeping of work that
		// has already been claimed; the request timeout bounds the network part.
		d.process(context.WithoutCancel(ctx), id, ev)
	}
}

// process runs one delivery attempt for a claimed event and applies the
// resulting state transition.
func (d *Dispatcher) process(ctx context.Context, workerID int, ev store.Event) {
	log := d.log.With(
		"event_id", ev.ID, "subscription_id", ev.SubscriptionID,
		"customer_id", ev.CustomerID, "worker", workerID,
	)
	now := d.now()

	// Terminal deadline (FR6). An event claimed at or after its deadline is
	// failed without another attempt, which is what bounds the retry storm.
	deadline := ev.CreatedAt.Add(d.cfg.Retry.Deadline)
	if !now.Before(deadline) {
		reason := "retry deadline exceeded"
		if ev.LastError != "" {
			reason += ": " + ev.LastError
		}
		if err := d.store.MarkFailed(ctx, ev.ID, reason, now); err != nil {
			log.Error("mark failed", "error", err)
			return
		}
		log.Warn("event failed", "reason", reason, "attempts", ev.AttemptCount)
		return
	}

	// Open breaker: do not burn an attempt, push the event to the moment the
	// breaker expects to be closed again and go claim something else. This is
	// what keeps a dead endpoint from parking workers or eating its backlog.
	if !d.breaker.Allow(ev.EndpointURL, now) {
		retryAt := d.breaker.RetryAt(ev.EndpointURL, now)
		if err := d.store.Reschedule(ctx, ev.ID, retryAt, "circuit breaker open"); err != nil {
			log.Error("reschedule for breaker", "error", err)
		}
		log.Debug("held by circuit breaker", "endpoint", ev.EndpointURL, "retry_at", retryAt)
		return
	}

	attempt := ev.AttemptCount + 1
	res := d.sender.Send(ctx, ev.EndpointURL, ev, attempt)

	// Every attempt leaves a row, whatever the outcome (status, error, duration).
	attemptErr := res.AttemptError()
	if err := d.store.RecordAttempt(ctx, ev.ID, store.Attempt{
		Attempt:     attempt,
		AttemptedAt: res.StartedAt,
		HTTPStatus:  statusCode(res),
		Error:       attemptErr,
		DurationMS:  res.Duration.Milliseconds(),
	}); err != nil {
		log.Error("record attempt", "error", err)
	}

	switch retry.Classify(res.StatusCode, res.Err) {
	case retry.OutcomeSuccess:
		d.breaker.RecordSuccess(ev.EndpointURL)
		if err := d.store.MarkDelivered(ctx, ev.ID, d.now()); err != nil {
			log.Error("mark delivered", "error", err)
			return
		}
		log.Info("event delivered", "attempt", attempt, "status", res.StatusCode, "duration", res.Duration)

	case retry.OutcomeFail:
		d.breaker.RecordFailure(ev.EndpointURL, now)
		reason := "permanent failure: " + attemptErr
		if err := d.store.MarkFailed(ctx, ev.ID, reason, d.now()); err != nil {
			log.Error("mark failed", "error", err)
			return
		}
		log.Warn("event failed", "reason", reason, "attempt", attempt)

	default: // retry.OutcomeRetry
		if d.breaker.RecordFailure(ev.EndpointURL, now) {
			log.Warn("circuit breaker opened",
				"endpoint", ev.EndpointURL,
				"consecutive_failures", d.breaker.ConsecutiveFailures(ev.EndpointURL),
				"retry_at", d.breaker.RetryAt(ev.EndpointURL, d.now()).Format(time.RFC3339Nano),
			)
		}

		delay := retry.Delay(attempt, retry.Params{
			Initial:    d.cfg.Retry.InitialDelay,
			Multiplier: d.cfg.Retry.Multiplier,
			Jitter:     d.cfg.Retry.Jitter,
			Max:        d.cfg.Retry.MaxDelay,
		}, d.rnd)
		source := "backoff"
		if res.HasRetryAfter {
			delay, source = res.RetryAfter, "retry-after"
		}
		if delay < d.cfg.MinRetryGap {
			// Never reschedule closer than MinRetryGap: a Retry-After: 0 (or an
			// aggressive backoff config) must not become a hot loop.
			delay = d.cfg.MinRetryGap
		}
		next := d.now().Add(delay)
		// Clamp at the deadline; the claim-time check above turns the clamped
		// event into terminal `failed` when the deadline passes.
		if next.After(deadline) {
			next = deadline
		}
		if err := d.store.Reschedule(ctx, ev.ID, next, attemptErr); err != nil {
			log.Error("reschedule event", "error", err)
			return
		}
		log.Warn("delivery attempt failed, retry scheduled",
			"attempt", attempt, "status", res.StatusCode,
			"reason", attemptErr, "delay_source", source, "delay", delay,
			"next_attempt_at", next.Format(time.RFC3339Nano),
		)
	}
}

func statusCode(res delivery.Result) *int {
	if res.StatusCode == 0 {
		return nil // no response: transport error
	}
	s := res.StatusCode
	return &s
}

// sleep waits for d or until ctx is cancelled. Returns false on cancellation.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
