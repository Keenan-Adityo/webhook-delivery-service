package breaker

import (
	"testing"
	"time"
)

func TestBreakerOpensAtThreshold(t *testing.T) {
	now := time.Now()
	b := New(3, 30*time.Second, 10*time.Minute)
	const endpoint = "http://example.test/hook"

	for i := 1; i < 3; i++ {
		if b.RecordFailure(endpoint, now) {
			t.Fatalf("opened after %d failures, threshold is 3", i)
		}
		if !b.Allow(endpoint, now) {
			t.Fatalf("closed breaker blocked request after %d failures", i)
		}
	}
	if !b.RecordFailure(endpoint, now) {
		t.Fatal("did not open at the threshold")
	}
	if b.Allow(endpoint, now) {
		t.Fatal("open breaker still allows requests")
	}
	if got := b.RetryAt(endpoint, now); !got.Equal(now.Add(30 * time.Second)) {
		t.Errorf("RetryAt = %s, want %s", got, now.Add(30*time.Second))
	}
}

func TestCoolDownGrowsAndIsCapped(t *testing.T) {
	now := time.Now()
	b := New(2, 10*time.Second, 60*time.Second)
	const endpoint = "http://example.test/hook"

	// Open at the threshold: 1x cool-down.
	b.RecordFailure(endpoint, now)
	b.RecordFailure(endpoint, now)
	if got := b.RetryAt(endpoint, now); !got.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("first cool-down = %s, want 10s", got.Sub(now))
	}

	// Each further consecutive failure doubles it, capped at 60s.
	next := now.Add(10 * time.Second)
	b.RecordFailure(endpoint, next) // 2x = 20s
	if got := b.RetryAt(endpoint, next); got.Sub(next) != 20*time.Second {
		t.Errorf("second cool-down = %s, want 20s", got.Sub(next))
	}
	next = next.Add(20 * time.Second)
	b.RecordFailure(endpoint, next) // 4x = 40s
	if got := b.RetryAt(endpoint, next); got.Sub(next) != 40*time.Second {
		t.Errorf("third cool-down = %s, want 40s", got.Sub(next))
	}
	next = next.Add(40 * time.Second)
	b.RecordFailure(endpoint, next) // 8x = 80s -> capped at 60s
	if got := b.RetryAt(endpoint, next); got.Sub(next) != 60*time.Second {
		t.Errorf("fourth cool-down = %s, want the 60s cap", got.Sub(next))
	}
}

func TestCoolDownExpiryAllowsTrafficAgain(t *testing.T) {
	now := time.Now()
	b := New(1, 10*time.Second, time.Minute)
	const endpoint = "http://example.test/hook"

	b.RecordFailure(endpoint, now)
	if b.Allow(endpoint, now.Add(9*time.Second)) {
		t.Fatal("allowed during cool-down")
	}
	if !b.Allow(endpoint, now.Add(10*time.Second)) {
		t.Fatal("still blocked after the cool-down: no half-open probe")
	}
}

func TestSuccessClosesBreaker(t *testing.T) {
	now := time.Now()
	b := New(2, 10*time.Second, time.Minute)
	const endpoint = "http://example.test/hook"

	b.RecordFailure(endpoint, now)
	b.RecordSuccess(endpoint)
	b.RecordFailure(endpoint, now) // streak restarted, so still below threshold
	if !b.Allow(endpoint, now) {
		t.Fatal("breaker opened although the failure streak was reset")
	}
	if got := b.ConsecutiveFailures(endpoint); got != 1 {
		t.Errorf("consecutive failures = %d, want 1", got)
	}
}

func TestEndpointsAreIndependent(t *testing.T) {
	now := time.Now()
	b := New(1, 10*time.Second, time.Minute)

	b.RecordFailure("http://a.test/hook", now)
	if b.Allow("http://a.test/hook", now) {
		t.Fatal("endpoint a should be open")
	}
	if !b.Allow("http://b.test/hook", now) {
		t.Fatal("endpoint b must not be affected by a's failures")
	}
}
