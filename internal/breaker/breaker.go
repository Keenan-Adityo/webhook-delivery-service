// Package breaker is a per-endpoint circuit breaker: after K consecutive
// failures the endpoint is paused for a growing cool-down instead of burning
// retries on every queued event (PRD 4.3).
package breaker

import (
	"sync"
	"time"
)

type state struct {
	consecutive int
	// openedAt + coolDown is the end of the current pause.
	openedAt time.Time
	coolDown time.Duration
}

type Breaker struct {
	threshold   int
	coolDown    time.Duration
	maxCoolDown time.Duration

	mu     sync.Mutex
	states map[string]*state
}

func New(threshold int, coolDown, maxCoolDown time.Duration) *Breaker {
	return &Breaker{
		threshold:   threshold,
		coolDown:    coolDown,
		maxCoolDown: maxCoolDown,
		states:      make(map[string]*state),
	}
}

// Allow reports whether a request to endpoint may be attempted at now.
// After an expired cool-down traffic is allowed again (a half-open probe): if
// the endpoint is still broken the next failure re-opens the breaker with a
// longer pause; if it recovered, RecordSuccess closes it for good.
func (b *Breaker) Allow(endpoint string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.states[endpoint]
	if st == nil || st.coolDown == 0 {
		return true
	}
	return !now.Before(st.openedAt.Add(st.coolDown))
}

// RetryAt returns when Allow becomes true again. Events held by an open
// breaker are rescheduled to this time so no worker waits on the endpoint.
func (b *Breaker) RetryAt(endpoint string, now time.Time) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.states[endpoint]
	if st == nil || st.coolDown == 0 {
		return now
	}
	return st.openedAt.Add(st.coolDown)
}

// RecordSuccess closes the breaker for endpoint.
func (b *Breaker) RecordSuccess(endpoint string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st := b.states[endpoint]; st != nil {
		st.consecutive = 0
		st.coolDown = 0
	}
}

// RecordFailure counts a consecutive failure and opens the breaker once the
// threshold is reached. Each further consecutive failure doubles the cool-down
// (capped): a broken endpoint is probed less and less often. Returns true when
// this failure opened (or re-opened) the breaker.
func (b *Breaker) RecordFailure(endpoint string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.states[endpoint]
	if st == nil {
		st = &state{}
		b.states[endpoint] = st
	}
	st.consecutive++
	if st.consecutive < b.threshold {
		return false
	}

	// consecutive == threshold -> 1x coolDown, threshold+1 -> 2x, ...
	shift := st.consecutive - b.threshold
	cd := b.coolDown
	for i := 0; i < shift && cd < b.maxCoolDown; i++ {
		cd *= 2
	}
	if cd > b.maxCoolDown {
		cd = b.maxCoolDown
	}
	st.openedAt = now
	st.coolDown = cd
	return true
}

// ConsecutiveFailures reports the current streak; used by tests and logs.
func (b *Breaker) ConsecutiveFailures(endpoint string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st := b.states[endpoint]; st != nil {
		return st.consecutive
	}
	return 0
}
