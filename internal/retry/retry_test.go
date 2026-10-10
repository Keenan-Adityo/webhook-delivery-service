package retry

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"testing"
	"time"
)

func TestDelayWithoutJitterIsExactExponentialBackoff(t *testing.T) {
	p := Params{
		Initial:    10 * time.Second,
		Multiplier: 2,
		Jitter:     0.2,
		Max:        30 * time.Minute,
	}
	// rnd = 0.5 makes the jitter factor exactly 1.
	neutral := func() float64 { return 0.5 }

	want := []time.Duration{
		10 * time.Second,  // attempt 1
		20 * time.Second,  // 2x
		40 * time.Second,  // 4x
		80 * time.Second,  // 8x
		160 * time.Second, // 16x
	}
	for i, w := range want {
		if got := Delay(i+1, p, neutral); got != w {
			t.Errorf("attempt %d: got %s, want %s", i+1, got, w)
		}
	}
}

func TestDelayIsCappedAtMax(t *testing.T) {
	p := Params{Initial: time.Minute, Multiplier: 2, Max: 30 * time.Minute}
	neutral := func() float64 { return 0.5 }
	for attempt := 1; attempt <= 20; attempt++ {
		if got := Delay(attempt, p, neutral); got > p.Max {
			t.Fatalf("attempt %d: delay %s exceeds max %s", attempt, got, p.Max)
		}
	}
	// Attempt 10 would be 512 minutes without the cap.
	if got := Delay(10, p, neutral); got != p.Max {
		t.Errorf("attempt 10: got %s, want the cap %s", got, p.Max)
	}
}

func TestDelayJitterStaysWithinBounds(t *testing.T) {
	p := Params{Initial: 10 * time.Second, Multiplier: 2, Jitter: 0.2, Max: 30 * time.Minute}
	base := float64(10 * time.Second)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		got := float64(Delay(1, p, rng.Float64))
		if got < base*0.8 || got > base*1.2 {
			t.Fatalf("jittered delay %s outside ±20%% of %s", time.Duration(got), time.Duration(base))
		}
	}

	// Extremes of the jitter range.
	if got := float64(Delay(1, p, func() float64 { return 0 })); got != base*0.8 {
		t.Errorf("minimum jitter: got %s, want %s", time.Duration(got), time.Duration(base*0.8))
	}
	if got := float64(Delay(1, p, func() float64 { return 0.9999999 })); got > base*1.2 {
		t.Errorf("maximum jitter: got %s, want <= %s", time.Duration(got), time.Duration(base*1.2))
	}
}

func TestDelayJitterNeverExceedsMax(t *testing.T) {
	// At the cap the jitter range is compressed rather than allowed to push
	// the delay past the configured maximum.
	p := Params{Initial: 30 * time.Minute, Multiplier: 2, Jitter: 0.2, Max: 30 * time.Minute}
	for _, r := range []float64{0, 0.5, 0.9999999} {
		rnd := r
		if got := Delay(5, p, func() float64 { return rnd }); got > p.Max {
			t.Errorf("rnd=%v: delay %s exceeds max %s", rnd, got, p.Max)
		}
	}
}

func TestDelaySurvivesOverflow(t *testing.T) {
	p := Params{Initial: 10 * time.Second, Multiplier: 2, Jitter: 0.2, Max: 30 * time.Minute}
	// 2^999 overflows float64; the result must stay at the cap, not NaN/Inf.
	got := Delay(1000, p, func() float64 { return 0.5 })
	if got != p.Max {
		t.Errorf("got %s, want %s", got, p.Max)
	}
}

func TestDelayTreatsLowAttemptCountsAsFirst(t *testing.T) {
	p := Params{Initial: 10 * time.Second, Multiplier: 2, Max: 30 * time.Minute}
	if got := Delay(0, p, func() float64 { return 0.5 }); got != 10*time.Second {
		t.Errorf("attempt 0: got %s, want 10s", got)
	}
	if got := Delay(-3, p, func() float64 { return 0.5 }); got != 10*time.Second {
		t.Errorf("attempt -3: got %s, want 10s", got)
	}
}

func TestClassify(t *testing.T) {
	transportErr := errors.New("dial tcp: connection refused")

	cases := []struct {
		name   string
		status int
		err    error
		want   Outcome
	}{
		{"network error retries", 0, transportErr, OutcomeRetry},
		{"timeout retries", 0, context.DeadlineExceeded, OutcomeRetry},
		{"200 succeeds", 200, nil, OutcomeSuccess},
		{"201 succeeds", 201, nil, OutcomeSuccess},
		{"204 succeeds", 204, nil, OutcomeSuccess},
		{"408 retries", 408, nil, OutcomeRetry},
		{"429 retries", 429, nil, OutcomeRetry},
		{"500 retries", 500, nil, OutcomeRetry},
		{"502 retries", 502, nil, OutcomeRetry},
		{"503 retries", 503, nil, OutcomeRetry},
		{"504 retries", 504, nil, OutcomeRetry},
		{"400 fails", 400, nil, OutcomeFail},
		{"401 fails", 401, nil, OutcomeFail},
		{"404 fails", 404, nil, OutcomeFail},
		{"409 fails", 409, nil, OutcomeFail},
		{"422 fails", 422, nil, OutcomeFail},
		{"301 retries", 301, nil, OutcomeRetry},
		{"302 retries", 302, nil, OutcomeRetry},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.status, tc.err); got != tc.want {
				t.Errorf("Classify(%d, %v) = %v, want %v", tc.status, tc.err, got, tc.want)
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

	t.Run("seconds", func(t *testing.T) {
		h := http.Header{"Retry-After": []string{"120"}}
		got, ok := RetryAfter(h, now)
		if !ok || got != 2*time.Minute {
			t.Errorf("got (%s, %v), want (2m, true)", got, ok)
		}
	})

	t.Run("http date", func(t *testing.T) {
		h := http.Header{"Retry-After": []string{now.Add(30 * time.Second).Format(http.TimeFormat)}}
		got, ok := RetryAfter(h, now)
		if !ok || got != 30*time.Second {
			t.Errorf("got (%s, %v), want (30s, true)", got, ok)
		}
	})

	t.Run("past date clamps to zero", func(t *testing.T) {
		h := http.Header{"Retry-After": []string{now.Add(-time.Hour).Format(http.TimeFormat)}}
		got, ok := RetryAfter(h, now)
		if !ok || got != 0 {
			t.Errorf("got (%s, %v), want (0, true)", got, ok)
		}
	})

	t.Run("absent", func(t *testing.T) {
		if got, ok := RetryAfter(http.Header{}, now); ok {
			t.Errorf("got (%s, true), want ok=false", got)
		}
	})

	t.Run("garbage", func(t *testing.T) {
		h := http.Header{"Retry-After": []string{"soon"}}
		if got, ok := RetryAfter(h, now); ok {
			t.Errorf("got (%s, true), want ok=false", got)
		}
	})

	t.Run("negative seconds", func(t *testing.T) {
		h := http.Header{"Retry-After": []string{"-5"}}
		if got, ok := RetryAfter(h, now); ok {
			t.Errorf("got (%s, true), want ok=false", got)
		}
	})
}
