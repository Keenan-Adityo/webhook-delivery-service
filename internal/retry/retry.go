// Package retry implements the locked retry policy: classification of an
// attempt outcome and the exponential backoff with jitter.
package retry

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Outcome of a single delivery attempt.
type Outcome int

const (
	// OutcomeSuccess: 2xx, the event is delivered (FR4).
	OutcomeSuccess Outcome = iota
	// OutcomeRetry: transient, schedule another attempt (FR5).
	OutcomeRetry
	// OutcomeFail: permanent, move to terminal `failed` now.
	OutcomeFail
)

// Classify decides what happens after one attempt.
//
//   - transport error (network failure, timeout): retry — we cannot know
//     whether the receiver processed it, and at-least-once demands trying again;
//   - 2xx: success;
//   - 5xx, 408, 429: retry, these are explicitly transient;
//   - any other 4xx: fail early — retrying a request the receiver calls
//     invalid only burns quota until the deadline;
//   - everything else (1xx/3xx): retry, per FR4 "any other response schedules
//     a retry". Redirects are not followed, so a 3xx lands here and is visible
//     in the attempt history.
func Classify(status int, err error) Outcome {
	if err != nil {
		return OutcomeRetry
	}
	switch {
	case status >= 200 && status < 300:
		return OutcomeSuccess
	case status == http.StatusRequestTimeout,
		status == http.StatusTooManyRequests,
		status >= 500:
		return OutcomeRetry
	case status >= 400 && status < 500:
		return OutcomeFail
	default:
		return OutcomeRetry
	}
}

// Params are the tunable backoff parameters (PRD defaults live in config).
type Params struct {
	Initial    time.Duration
	Multiplier float64
	// Jitter is the ± fraction of the delay, e.g. 0.2 for ±20%.
	Jitter float64
	Max    time.Duration
}

// Delay returns how long to wait before the next attempt, given the number of
// attempts already made (>= 1):
//
//	base    = Initial * Multiplier^(attempts-1), capped at Max
//	delayed = base * (1 + (2*rnd()-1)*Jitter), then capped at Max again
//
// The second cap means a jittered delay never exceeds Max; at the cap the
// jitter range is therefore compressed, which is the lesser evil compared to
// sleeping past the configured maximum. rnd must return a value in [0,1); a
// nil rnd means no jitter (useful in tests).
func Delay(attempts int, p Params, rnd func() float64) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	base := float64(p.Initial) * math.Pow(p.Multiplier, float64(attempts-1))
	// math.Pow can overflow to +Inf for large attempt counts; the cap handles it.
	if math.IsNaN(base) || math.IsInf(base, 0) || base > float64(p.Max) {
		base = float64(p.Max)
	}
	delay := base
	if p.Jitter > 0 && rnd != nil {
		delay = base * (1 + (2*rnd()-1)*p.Jitter)
	}
	if delay < 0 {
		delay = 0
	}
	if delay > float64(p.Max) {
		delay = float64(p.Max)
	}
	return time.Duration(delay)
}

// RetryAfter parses the Retry-After header (delay in seconds or an HTTP date)
// and returns the wait relative to now. ok is false when the header is absent
// or unusable, in which case the caller falls back to exponential backoff.
func RetryAfter(h http.Header, now time.Time) (delay time.Duration, ok bool) {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
