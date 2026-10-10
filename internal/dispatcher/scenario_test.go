package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"webhook-delivery-service/internal/breaker"
	"webhook-delivery-service/internal/config"
	"webhook-delivery-service/internal/delivery"
	"webhook-delivery-service/internal/mockendpoint"
	"webhook-delivery-service/internal/store"
)

// testConfig scales every time-based parameter down (as the PRD asks) so the
// scenarios run in milliseconds instead of hours, while keeping the shape of
// the production defaults: backoff grows, the breaker has a threshold, the
// deadline exists.
func testConfig() config.Config {
	cfg := config.Default()
	cfg.Workers = 4
	cfg.PerCustomerCap = 2
	cfg.PollInterval = 5 * time.Millisecond
	cfg.LeaseDuration = 500 * time.Millisecond
	cfg.RequestTimeout = 100 * time.Millisecond
	cfg.ShutdownTimeout = 3 * time.Second
	cfg.MinRetryGap = 20 * time.Millisecond
	cfg.Retry.InitialDelay = 20 * time.Millisecond
	cfg.Retry.MaxDelay = 100 * time.Millisecond
	cfg.Retry.Deadline = 2 * time.Second
	cfg.Breaker.Threshold = 3
	cfg.Breaker.CoolDown = 200 * time.Millisecond
	cfg.Breaker.MaxCoolDown = 800 * time.Millisecond
	return cfg
}

type harness struct {
	t       *testing.T
	cfg     config.Config
	store   *store.Store
	breaker *breaker.Breaker
}

// newHarness opens the store at dbPath ("" = fresh temp file) and starts the
// worker pool. Shutdown is asserted in cleanup: no goroutine may leak.
func newHarness(t *testing.T, dbPath string, tune func(*config.Config)) *harness {
	t.Helper()
	cfg := testConfig()
	if tune != nil {
		tune(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config: %v", err)
	}
	if dbPath == "" {
		dbPath = filepath.Join(t.TempDir(), "dispatch.db")
	}
	st, err := store.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	brk := breaker.New(cfg.Breaker.Threshold, cfg.Breaker.CoolDown, cfg.Breaker.MaxCoolDown)
	d := New(cfg, st, delivery.NewSender(cfg.RequestTimeout, nil), brk,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("dispatcher did not stop within the shutdown timeout")
		}
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return &harness{t: t, cfg: cfg, store: st, breaker: brk}
}

func (h *harness) endpoint(opts mockendpoint.Options) (*httptest.Server, *mockendpoint.Handler) {
	h.t.Helper()
	mock := mockendpoint.New(opts)
	srv := httptest.NewServer(mock)
	h.t.Cleanup(srv.Close)
	return srv, mock
}

func (h *harness) subscribe(customer, url string) store.Subscription {
	h.t.Helper()
	sub, err := h.store.CreateSubscription(context.Background(), customer, url)
	if err != nil {
		h.t.Fatalf("create subscription: %v", err)
	}
	return sub
}

func (h *harness) emit(subID, payload string) store.Event {
	h.t.Helper()
	ev, err := h.store.CreateEvent(context.Background(), subID, json.RawMessage(payload))
	if err != nil {
		h.t.Fatalf("create event: %v", err)
	}
	return ev
}

func (h *harness) event(id string) store.Event {
	h.t.Helper()
	ev, err := h.store.GetEvent(context.Background(), id)
	if err != nil {
		h.t.Fatalf("get event %s: %v", id, err)
	}
	return ev
}

func (h *harness) waitFor(timeout time.Duration, what string, cond func() bool) {
	h.t.Helper()
	waitFor(h.t, timeout, what, cond)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// Scenario 1 (PRD 9): delivered exactly once, with the required wire format.
func TestHappyPathDeliversOnce(t *testing.T) {
	h := newHarness(t, "", nil)
	srv, mock := h.endpoint(mockendpoint.Options{Mode: mockendpoint.ModeOK})
	sub := h.subscribe("acme", srv.URL)
	ev := h.emit(sub.ID, `{"hello":"world"}`)

	h.waitFor(5*time.Second, "event delivered", func() bool {
		return h.event(ev.ID).Status == store.StatusDelivered
	})

	got := h.event(ev.ID)
	if got.AttemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.DeliveredAt == nil {
		t.Error("delivered_at missing")
	}

	reqs := mock.Requests()
	if len(reqs) != 1 {
		t.Fatalf("endpoint received %d requests, want exactly 1", len(reqs))
	}
	r := reqs[0]
	if r.EventID != ev.ID {
		t.Errorf("X-Event-Id = %q, want %q", r.EventID, ev.ID)
	}
	if r.Attempt != "1" {
		t.Errorf("X-Delivery-Attempt = %q, want 1", r.Attempt)
	}
	if r.ContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", r.ContentType)
	}
	var body struct {
		EventID   string          `json:"event_id"`
		CreatedAt time.Time       `json:"created_at"`
		Sequence  int64           `json:"sequence"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(r.Body, &body); err != nil {
		t.Fatalf("delivery body is not JSON: %v (%s)", err, r.Body)
	}
	if body.EventID != ev.ID || body.Sequence != 1 || string(body.Payload) != `{"hello":"world"}` {
		t.Errorf("delivery body = %s", r.Body)
	}
	if body.CreatedAt.IsZero() {
		t.Error("delivery body missing created_at")
	}
}

// Scenario 2: the endpoint is down at first, then recovers; the attempt
// history shows retries with growing backoff.
func TestEndpointDownThenRecovers(t *testing.T) {
	h := newHarness(t, "", func(c *config.Config) { c.Retry.Deadline = 5 * time.Second })
	srv, mock := h.endpoint(mockendpoint.Options{Mode: mockendpoint.ModeFlaky, FailCount: 3})
	sub := h.subscribe("acme", srv.URL)
	ev := h.emit(sub.ID, `{"n":1}`)

	h.waitFor(10*time.Second, "event delivered after recovery", func() bool {
		return h.event(ev.ID).Status == store.StatusDelivered
	})

	got := h.event(ev.ID)
	if got.AttemptCount != 4 {
		t.Fatalf("attempt_count = %d, want 4 (3 failures + 1 success)", got.AttemptCount)
	}
	attempts := got.Attempts
	if len(attempts) != 4 {
		t.Fatalf("attempt history = %d rows, want 4", len(attempts))
	}
	wantStatus := []int{500, 500, 500, 200}
	for i, want := range wantStatus {
		if attempts[i].HTTPStatus == nil || *attempts[i].HTTPStatus != want {
			t.Errorf("attempt %d status = %v, want %d", i+1, attempts[i].HTTPStatus, want)
		}
	}
	// Backoff visible in the history: 20ms then 40ms (±20% jitter), with
	// generous lower bounds so a slow machine cannot fail the test.
	gap1 := attempts[1].AttemptedAt.Sub(attempts[0].AttemptedAt)
	gap2 := attempts[2].AttemptedAt.Sub(attempts[1].AttemptedAt)
	if gap1 < 12*time.Millisecond {
		t.Errorf("gap between attempt 1 and 2 = %s, want >= 12ms (initial delay 20ms)", gap1)
	}
	if gap2 < 25*time.Millisecond {
		t.Errorf("gap between attempt 2 and 3 = %s, want >= 25ms (second backoff 40ms)", gap2)
	}
	if gap2 <= gap1 {
		t.Errorf("backoff did not grow: %s then %s", gap1, gap2)
	}
	if mock.Count() != 4 {
		t.Errorf("endpoint received %d requests, want 4", mock.Count())
	}
}

// Scenario 3: a long outage of one customer must not delay another customer,
// the breaker must open, and the stuck events must end `failed` at the deadline.
func TestLongOutageIsolatesOtherCustomers(t *testing.T) {
	h := newHarness(t, "", func(c *config.Config) {
		c.Retry.Deadline = 1200 * time.Millisecond
		c.Breaker.Threshold = 3
		c.Breaker.CoolDown = 400 * time.Millisecond
	})
	badSrv, badMock := h.endpoint(mockendpoint.Options{Mode: mockendpoint.ModeFail})
	goodSrv, goodMock := h.endpoint(mockendpoint.Options{Mode: mockendpoint.ModeOK})

	badSub := h.subscribe("acme", badSrv.URL)
	goodSub := h.subscribe("globex", goodSrv.URL)

	stuckA := h.emit(badSub.ID, `{"n":1}`)
	stuckB := h.emit(badSub.ID, `{"n":2}`)

	// The healthy customer stays timely while acme is failing.
	start := time.Now()
	healthy := h.emit(goodSub.ID, `{"n":3}`)
	h.waitFor(2*time.Second, "globex delivered during acme's outage", func() bool {
		return h.event(healthy.ID).Status == store.StatusDelivered
	})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("healthy customer took %s for a fresh event while acme was down", elapsed)
	}
	// ...and keeps being timely on a second event, while acme is still broken.
	healthy2 := h.emit(goodSub.ID, `{"n":4}`)
	h.waitFor(2*time.Second, "second globex event delivered", func() bool {
		return h.event(healthy2.ID).Status == store.StatusDelivered
	})

	// The breaker opens for the failing endpoint.
	h.waitFor(3*time.Second, "circuit breaker to open", func() bool {
		return !h.breaker.Allow(badSrv.URL, time.Now())
	})
	if h.breaker.ConsecutiveFailures(badSrv.URL) < h.cfg.Breaker.Threshold {
		t.Errorf("consecutive failures = %d, want >= %d",
			h.breaker.ConsecutiveFailures(badSrv.URL), h.cfg.Breaker.Threshold)
	}

	// Past the deadline the stuck events become terminal `failed` and stay.
	h.waitFor(5*time.Second, "stuck events to fail after the deadline", func() bool {
		return h.event(stuckA.ID).Status == store.StatusFailed &&
			h.event(stuckB.ID).Status == store.StatusFailed
	})
	for _, id := range []string{stuckA.ID, stuckB.ID} {
		got := h.event(id)
		if !strings.Contains(got.LastError, "retry deadline exceeded") {
			t.Errorf("event %s last_error = %q, want it to mention the deadline", id, got.LastError)
		}
		if got.AttemptCount < 1 {
			t.Errorf("event %s has no attempts", id)
		}
	}

	counts, err := h.store.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts.Failed != 2 || counts.Delivered != 2 {
		t.Errorf("counts = %+v, want 2 failed (acme) and 2 delivered (globex)", counts)
	}
	if badMock.Count() == 0 || goodMock.Count() == 0 {
		t.Errorf("both endpoints should have been called: bad=%d good=%d", badMock.Count(), goodMock.Count())
	}
}

// Scenario 4: a slow endpoint releases workers at the request timeout and the
// per-customer cap holds, so one customer cannot occupy the whole pool.
func TestSlowEndpointReleasesWorkersAndCapHolds(t *testing.T) {
	h := newHarness(t, "", func(c *config.Config) {
		c.Workers = 4
		c.PerCustomerCap = 2
		c.RequestTimeout = 100 * time.Millisecond
		c.LeaseDuration = 500 * time.Millisecond
		// Larger than the handler delay, so a request abandoned at the timeout
		// has definitely returned before the next one starts: the observed
		// concurrency is exactly the cap, with no overlap from dead requests.
		c.MinRetryGap = 250 * time.Millisecond
		c.Retry.Deadline = 1500 * time.Millisecond
		// The breaker is exercised by its own scenario; here it would only
		// defer first attempts of the backlog and muddy the cap assertion.
		c.Breaker.Threshold = 1000
	})
	srv, mock := h.endpoint(mockendpoint.Options{
		Mode:  mockendpoint.ModeSlow,
		Delay: 150 * time.Millisecond, // well beyond the 100ms request timeout
	})
	sub := h.subscribe("slowco", srv.URL)

	var ids []string
	for i := 0; i < 6; i++ {
		ids = append(ids, h.emit(sub.ID, fmt.Sprintf(`{"n":%d}`, i)).ID)
	}

	// Workers are released at the timeout: every event gets attempted, none is
	// parked waiting on the endpoint.
	h.waitFor(3*time.Second, "every event attempted at least once", func() bool {
		for _, id := range ids {
			if h.event(id).AttemptCount < 1 {
				return false
			}
		}
		return true
	})

	// All of them reach the terminal state after the deadline instead of
	// hanging forever.
	h.waitFor(6*time.Second, "all slow-endpoint events to end failed", func() bool {
		for _, id := range ids {
			if h.event(id).Status != store.StatusFailed {
				return false
			}
		}
		return true
	})

	if got := mock.MaxInFlight(); got > h.cfg.PerCustomerCap {
		t.Errorf("peak concurrent requests = %d, want <= per-customer cap %d", got, h.cfg.PerCustomerCap)
	}

	// The recorded reason is the per-request timeout, not a hang.
	var sawTimeout bool
	for _, id := range ids {
		ev := h.event(id)
		if strings.Contains(ev.LastError, "deadline exceeded") {
			sawTimeout = true
		}
		if len(ev.Attempts) == 0 {
			t.Errorf("event %s has no attempt history", id)
		}
	}
	if !sawTimeout {
		t.Error("no event recorded a request timeout")
	}
}

// Scenario 5: a crash mid-delivery leaves a lease behind; after restart the
// event is picked up again once the lease expires. The delivery the dead
// process already made is a duplicate — acceptable under at-least-once, and
// event_id is the idempotency key for the customer.
func TestCrashMidDeliveryIsRetriedAfterLeaseExpiry(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "crash.db")
	mock := mockendpoint.New(mockendpoint.Options{Mode: mockendpoint.ModeOK})
	srv := httptest.NewServer(mock)
	defer srv.Close()

	// Phase 1: the worker claims the event and the request reaches the
	// endpoint, then the process dies before recording anything.
	st1, err := store.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	sub, err := st1.CreateSubscription(context.Background(), "acme", srv.URL)
	if err != nil {
		t.Fatalf("subscription: %v", err)
	}
	ev, err := st1.CreateEvent(context.Background(), sub.ID, json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("event: %v", err)
	}
	claimAt := time.Now()
	lease := claimAt.Add(1500 * time.Millisecond)
	claimed, ok, err := st1.ClaimDue(context.Background(), claimAt, lease, 2)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if claimed.ID != ev.ID {
		t.Fatalf("claimed %s, want %s", claimed.ID, ev.ID)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(`{"event_id":"`+ev.ID+`"}`))
	if err != nil {
		t.Fatalf("build in-flight request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-Id", ev.ID)
	req.Header.Set("X-Delivery-Attempt", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("simulate in-flight request: %v", err)
	}
	_ = resp.Body.Close()
	if err := st1.Close(); err != nil {
		t.Fatalf("crash (close store): %v", err)
	}

	// Phase 2: restart. While the lease is live nothing may be re-claimed.
	h := newHarness(t, dbPath, nil)
	if got := mock.Count(); got != 1 {
		t.Fatalf("requests right after restart = %d, want 1 (the pre-crash one)", got)
	}

	h.waitFor(5*time.Second, "event redelivered after the lease expired", func() bool {
		return h.event(ev.ID).Status == store.StatusDelivered
	})

	reqs := mock.Requests()
	if len(reqs) != 2 {
		t.Fatalf("endpoint received %d requests, want 2 (duplicate is acceptable)", len(reqs))
	}
	if reqs[0].EventID != ev.ID || reqs[1].EventID != ev.ID {
		t.Errorf("event ids = %q, %q, want %q on both (idempotency key)", reqs[0].EventID, reqs[1].EventID, ev.ID)
	}
	if reqs[1].ReceivedAt.Before(lease.Add(-10 * time.Millisecond)) {
		t.Errorf("redelivered at %s, before the lease expired at %s", reqs[1].ReceivedAt, lease)
	}

	got := h.event(ev.ID)
	if got.AttemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1: only attempts this process made are recorded", got.AttemptCount)
	}
}
