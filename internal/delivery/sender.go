// Package delivery performs the actual HTTP POST to a customer endpoint and
// reports the outcome for attempt recording.
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"webhook-delivery-service/internal/retry"
	"webhook-delivery-service/internal/store"
)

// snippetLimit bounds how much of an error response body is kept for the
// attempt history: enough to debug, small enough to store.
const snippetLimit = 512

// Result is the outcome of one attempt.
type Result struct {
	// StartedAt is when the attempt began; stored as attempted_at.
	StartedAt time.Time
	Duration  time.Duration
	// StatusCode is 0 when the request never got a response.
	StatusCode int
	// Err is the transport error (dial failure, timeout, ...).
	Err error
	// BodySnippet is a bounded copy of the response body, for diagnostics.
	BodySnippet string
	// RetryAfter comes from the response headers, when usable.
	RetryAfter    time.Duration
	HasRetryAfter bool
}

// AttemptError renders the human-readable reason stored in last_error and in
// delivery_attempts.error. Empty on success.
func (r Result) AttemptError() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	if r.StatusCode < 200 || r.StatusCode > 299 {
		s := fmt.Sprintf("HTTP %d", r.StatusCode)
		if r.BodySnippet != "" {
			s += ": " + r.BodySnippet
		}
		return s
	}
	return ""
}

type Sender struct {
	client  *http.Client
	timeout time.Duration
	now     func() time.Time
}

// NewSender builds a sender with the per-request timeout. now is the injected
// clock; nil means time.Now.
func NewSender(timeout time.Duration, now func() time.Time) *Sender {
	if now == nil {
		now = time.Now
	}
	return &Sender{
		client: &http.Client{
			Timeout: timeout,
			// Never follow redirects: a 3xx is an answer that must be
			// classified and recorded, and following would silently POST the
			// payload to a different host than the customer registered.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		timeout: timeout,
		now:     now,
	}
}

// body is the exact payload customers receive (PRD section 7).
type body struct {
	EventID   string          `json:"event_id"`
	CreatedAt time.Time       `json:"created_at"`
	Sequence  int64           `json:"sequence"`
	Payload   json.RawMessage `json:"payload"`
}

// Send POSTs the event to endpoint. attempt is the 1-based attempt number,
// reported in the X-Delivery-Attempt header.
//
// The request runs on a context that is not cancelled by shutdown (so
// graceful shutdown waits for in-flight requests) but is always bounded by the
// per-request timeout, which is what releases a worker parked on a slow
// endpoint.
func (s *Sender) Send(ctx context.Context, endpoint string, ev store.Event, attempt int) Result {
	payload, err := json.Marshal(body{
		EventID:   ev.ID,
		CreatedAt: ev.CreatedAt.UTC(),
		Sequence:  ev.Sequence,
		Payload:   ev.Payload,
	})
	if err != nil {
		// Only possible with a broken payload; not a transport failure.
		return Result{StartedAt: s.now(), Err: fmt.Errorf("encode delivery body: %w", err)}
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return Result{StartedAt: s.now(), Err: fmt.Errorf("build request: %w", err)}
	}
	req.Header.Set("X-Event-Id", ev.ID)
	req.Header.Set("X-Delivery-Attempt", strconv.Itoa(attempt))
	req.Header.Set("Content-Type", "application/json")

	started := s.now()
	resp, err := s.client.Do(req)
	if err != nil {
		return Result{
			StartedAt: started,
			Duration:  s.now().Sub(started),
			Err:       fmt.Errorf("post %s: %w", endpoint, err),
		}
	}
	defer resp.Body.Close()

	res := Result{
		StartedAt:  started,
		Duration:   s.now().Sub(started),
		StatusCode: resp.StatusCode,
	}
	// Bounded drain: enough for a useful error snippet, and it lets the
	// connection be reused instead of leaked.
	if snippet, _ := io.ReadAll(io.LimitReader(resp.Body, snippetLimit)); len(snippet) > 0 {
		res.BodySnippet = string(snippet)
	}
	if d, ok := retry.RetryAfter(resp.Header, s.now()); ok {
		res.RetryAfter, res.HasRetryAfter = d, true
	}
	return res
}
