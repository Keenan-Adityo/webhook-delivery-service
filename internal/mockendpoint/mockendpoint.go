// Package mockendpoint is the fake customer endpoint used by the scenario
// tests and by cmd/mockendpoint. Behaviour is switchable per PRD section 9.
package mockendpoint

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

type Mode string

const (
	// ModeOK always answers 200.
	ModeOK Mode = "ok"
	// ModeFail always answers 500.
	ModeFail Mode = "fail"
	// ModeSlow sleeps Delay (longer than the request timeout) then answers 200.
	ModeSlow Mode = "slow"
	// ModeFlaky answers 500 for the first FailCount requests, then 200 —
	// deterministic "intermittent failure" that recovers.
	ModeFlaky Mode = "flaky"
	// ModeDown answers 503 until DownFor has elapsed since the handler started,
	// then 200 — the "down for N, then recovers" scenario.
	ModeDown Mode = "down"
)

type Options struct {
	Mode      Mode
	Delay     time.Duration // ModeSlow
	FailCount int           // ModeFlaky
	DownFor   time.Duration // ModeDown
	// Now is the injected clock; nil means time.Now.
	Now func() time.Time
}

// Recorded is one received delivery request, kept for assertions.
type Recorded struct {
	EventID     string
	Attempt     string
	ContentType string
	Body        json.RawMessage
	ReceivedAt  time.Time
	Status      int
}

type Handler struct {
	opts  Options
	clock func() time.Time
	mu    sync.Mutex
	// started is fixed at construction so ModeDown recovers N after start.
	started  time.Time
	requests []Recorded
	inFlight int
	// maxInFlight is the observed peak of concurrent requests: the assertion
	// surface for "workers released at timeout" and the per-customer cap.
	maxInFlight int
}

func New(opts Options) *Handler {
	if opts.Mode == "" {
		opts.Mode = ModeOK
	}
	clock := opts.Now
	if clock == nil {
		clock = time.Now
	}
	if opts.FailCount <= 0 {
		opts.FailCount = 2
	}
	return &Handler{opts: opts, clock: clock, started: clock()}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))

	now := h.clock()
	h.mu.Lock()
	h.inFlight++
	if h.inFlight > h.maxInFlight {
		h.maxInFlight = h.inFlight
	}
	seq := len(h.requests) + 1
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		h.inFlight--
		h.mu.Unlock()
	}()

	status := h.decide(seq, now)
	if h.opts.Mode == ModeSlow {
		// Interruptible sleep: the client's timeout must be able to cut it.
		t := time.NewTimer(h.opts.Delay)
		defer t.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
		}
	}

	var body json.RawMessage
	if len(raw) > 0 {
		body = append(json.RawMessage(nil), raw...)
	}
	h.mu.Lock()
	h.requests = append(h.requests, Recorded{
		EventID:     r.Header.Get("X-Event-Id"),
		Attempt:     r.Header.Get("X-Delivery-Attempt"),
		ContentType: r.Header.Get("Content-Type"),
		Body:        body,
		ReceivedAt:  now,
		Status:      status,
	})
	h.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status < 300 {
		_, _ = w.Write([]byte(`{"ok":true}`))
	} else {
		_, _ = w.Write([]byte(`{"ok":false}`))
	}
}

func (h *Handler) decide(seq int, now time.Time) int {
	switch h.opts.Mode {
	case ModeFail:
		return http.StatusInternalServerError
	case ModeSlow:
		return http.StatusOK
	case ModeFlaky:
		if seq <= h.opts.FailCount {
			return http.StatusInternalServerError
		}
		return http.StatusOK
	case ModeDown:
		if now.Before(h.started.Add(h.opts.DownFor)) {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	default:
		return http.StatusOK
	}
}

// Requests returns a copy of everything received so far.
func (h *Handler) Requests() []Recorded {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Recorded(nil), h.requests...)
}

func (h *Handler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.requests)
}

func (h *Handler) MaxInFlight() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxInFlight
}
