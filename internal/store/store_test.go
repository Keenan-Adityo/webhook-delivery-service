package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// testClock is an injected, manually advanced clock.
type testClock struct{ t time.Time }

func (c *testClock) now() time.Time      { return c.t }
func (c *testClock) add(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore(t *testing.T, clock *testClock) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"), clock.now)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return st
}

func mustSub(t *testing.T, st *Store, customer, url string) Subscription {
	t.Helper()
	sub, err := st.CreateSubscription(context.Background(), customer, url)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	return sub
}

func mustEvent(t *testing.T, st *Store, subID, payload string) Event {
	t.Helper()
	ev, err := st.CreateEvent(context.Background(), subID, json.RawMessage(payload))
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	return ev
}

func TestCreateEventRejectsUnknownSubscription(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)

	_, err := st.CreateEvent(context.Background(), "sub_missing", json.RawMessage(`{}`))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	counts, err := st.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts != (Counts{}) {
		t.Errorf("nothing should be stored, got %+v", counts)
	}
}

func TestSequenceIsMonotonicPerCustomer(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)

	a1 := mustSub(t, st, "acme", "http://a.test/1")
	a2 := mustSub(t, st, "acme", "http://a.test/2")
	b1 := mustSub(t, st, "globex", "http://b.test/1")

	got := []int64{
		mustEvent(t, st, a1.ID, `{"n":1}`).Sequence,
		mustEvent(t, st, a2.ID, `{"n":2}`).Sequence, // different sub, same customer
		mustEvent(t, st, b1.ID, `{"n":3}`).Sequence,
		mustEvent(t, st, a1.ID, `{"n":4}`).Sequence,
	}
	want := []int64{1, 2, 1, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sequence %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

func TestClaimDueReturnsOldestAndSetsLease(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)
	sub := mustSub(t, st, "acme", "http://a.test")

	first := mustEvent(t, st, sub.ID, `{"n":1}`)
	clock.add(time.Second)
	mustEvent(t, st, sub.ID, `{"n":2}`) // second, also due

	claimed, ok, err := st.ClaimDue(context.Background(), clock.now(), clock.now().Add(time.Minute), 10)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if claimed.ID != first.ID {
		t.Errorf("claimed %s, want the oldest event %s", claimed.ID, first.ID)
	}
	if claimed.LeaseUntil == nil || !claimed.LeaseUntil.Equal(clock.now().Add(time.Minute)) {
		t.Errorf("lease_until = %v, want now+1m", claimed.LeaseUntil)
	}
	if claimed.EndpointURL != "http://a.test" || claimed.CustomerID != "acme" {
		t.Errorf("claim join missing data: %+v", claimed)
	}
}

func TestClaimDueSkipsFutureAndLeasedRows(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)
	sub := mustSub(t, st, "acme", "http://a.test")
	ev := mustEvent(t, st, sub.ID, `{}`)

	// Not due yet.
	clock.add(-time.Second) // claim time before created_at
	if _, ok, err := st.ClaimDue(context.Background(), clock.now(), clock.now().Add(time.Minute), 10); err != nil || ok {
		t.Fatalf("claimed a future event: ok=%v err=%v", ok, err)
	}
	clock.add(time.Second)

	// Due but leased by a live worker.
	if _, ok, err := st.ClaimDue(context.Background(), clock.now(), clock.now().Add(time.Minute), 10); err != nil || !ok {
		t.Fatalf("first claim failed: ok=%v err=%v", ok, err)
	}
	if _, ok, err := st.ClaimDue(context.Background(), clock.now(), clock.now().Add(time.Minute), 10); err != nil || ok {
		t.Fatalf("claimed a leased event: ok=%v err=%v", ok, err)
	}

	// Lease expired (crash): claimable again.
	clock.add(2 * time.Minute)
	again, ok, err := st.ClaimDue(context.Background(), clock.now(), clock.now().Add(time.Minute), 10)
	if err != nil || !ok {
		t.Fatalf("expired lease was not reclaimable: ok=%v err=%v", ok, err)
	}
	if again.ID != ev.ID {
		t.Errorf("reclaimed %s, want %s", again.ID, ev.ID)
	}
}

func TestClaimDueEnforcesPerCustomerCap(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)
	acme := mustSub(t, st, "acme", "http://a.test")
	globex := mustSub(t, st, "globex", "http://b.test")

	acmeA := mustEvent(t, st, acme.ID, `{"n":1}`)
	clock.add(time.Second)
	acmeB := mustEvent(t, st, acme.ID, `{"n":2}`)
	clock.add(time.Second)
	globexA := mustEvent(t, st, globex.ID, `{"n":3}`)

	lease := clock.now().Add(time.Minute)
	cap1 := 1

	first, ok, err := st.ClaimDue(context.Background(), clock.now(), lease, cap1)
	if err != nil || !ok {
		t.Fatalf("claim 1: ok=%v err=%v", ok, err)
	}
	if first.ID != acmeA.ID {
		t.Fatalf("claim 1 = %s, want acme's oldest %s", first.ID, acmeA.ID)
	}

	// Cap is 1 for acme, so the next claim must skip acmeB entirely.
	second, ok, err := st.ClaimDue(context.Background(), clock.now(), lease, cap1)
	if err != nil || !ok {
		t.Fatalf("claim 2: ok=%v err=%v", ok, err)
	}
	if second.ID != globexA.ID {
		t.Fatalf("claim 2 = %s (customer %s), want globex %s", second.ID, second.CustomerID, globexA.ID)
	}

	// Cap reached for acme and globex: nothing else is claimable.
	if _, ok, err := st.ClaimDue(context.Background(), clock.now(), lease, cap1); err != nil || ok {
		t.Fatalf("third claim should find nothing: ok=%v err=%v", ok, err)
	}

	// acmeA finishes and every lease expires: acme's backlog drains next.
	if err := st.MarkDelivered(context.Background(), acmeA.ID, clock.now()); err != nil {
		t.Fatalf("mark delivered: %v", err)
	}
	clock.add(2 * time.Minute)
	third, ok, err := st.ClaimDue(context.Background(), clock.now(), clock.now().Add(time.Minute), cap1)
	if err != nil || !ok {
		t.Fatalf("claim after expiry: ok=%v err=%v", ok, err)
	}
	if third.ID != acmeB.ID {
		t.Errorf("claim 3 = %s, want %s", third.ID, acmeB.ID)
	}
}

func TestRecordAttemptAndTerminalStates(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)
	sub := mustSub(t, st, "acme", "http://a.test")
	ev := mustEvent(t, st, sub.ID, `{"x":1}`)

	status := 500
	if err := st.RecordAttempt(context.Background(), ev.ID, Attempt{
		Attempt: 1, AttemptedAt: clock.now(), HTTPStatus: &status,
		Error: "HTTP 500", DurationMS: 12,
	}); err != nil {
		t.Fatalf("record attempt: %v", err)
	}

	got, err := st.GetEvent(context.Background(), ev.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if got.AttemptCount != 1 || got.Status != StatusPending {
		t.Errorf("attempt_count=%d status=%s, want 1/pending", got.AttemptCount, got.Status)
	}
	if len(got.Attempts) != 1 {
		t.Fatalf("attempt history = %d rows, want 1", len(got.Attempts))
	}
	a := got.Attempts[0]
	if a.HTTPStatus == nil || *a.HTTPStatus != 500 || a.Error != "HTTP 500" || a.DurationMS != 12 {
		t.Errorf("attempt row = %+v", a)
	}

	// Reschedule keeps it pending with a new next_attempt_at and clears lease.
	next := clock.now().Add(5 * time.Second)
	if err := st.Reschedule(context.Background(), ev.ID, next, "HTTP 500"); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	got, _ = st.GetEvent(context.Background(), ev.ID)
	if got.Status != StatusPending || !got.NextAttemptAt.Equal(next) || got.LastError != "HTTP 500" {
		t.Errorf("after reschedule: %+v", got)
	}

	// Delivered is terminal and recorded.
	if err := st.MarkDelivered(context.Background(), ev.ID, clock.now()); err != nil {
		t.Fatalf("mark delivered: %v", err)
	}
	got, _ = st.GetEvent(context.Background(), ev.ID)
	if got.Status != StatusDelivered || got.DeliveredAt == nil {
		t.Errorf("after delivered: status=%s delivered_at=%v", got.Status, got.DeliveredAt)
	}

	// Terminal rows are immutable: a stale worker cannot flip them back.
	if err := st.Reschedule(context.Background(), ev.ID, clock.now().Add(time.Second), "late"); !errors.Is(err, ErrNotFound) {
		t.Errorf("rescheduling a delivered event: got %v, want ErrNotFound", err)
	}
}

func TestMarkFailedKeepsRowAndCounts(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)
	sub := mustSub(t, st, "acme", "http://a.test")
	ev := mustEvent(t, st, sub.ID, `{}`)

	if err := st.MarkFailed(context.Background(), ev.ID, "retry deadline exceeded", clock.now()); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	got, err := st.GetEvent(context.Background(), ev.ID)
	if err != nil {
		t.Fatalf("failed events must never be deleted: %v", err)
	}
	if got.Status != StatusFailed || got.LastError != "retry deadline exceeded" {
		t.Errorf("got status=%s last_error=%q", got.Status, got.LastError)
	}
	counts, err := st.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts.Failed != 1 || counts.Pending != 0 {
		t.Errorf("counts = %+v, want 1 failed", counts)
	}
}

func TestListEventsFilters(t *testing.T) {
	clock := &testClock{t: time.Now()}
	st := newTestStore(t, clock)
	acme := mustSub(t, st, "acme", "http://a.test")
	globex := mustSub(t, st, "globex", "http://b.test")

	e2 := mustEvent(t, st, acme.ID, `{}`)
	mustEvent(t, st, acme.ID, `{}`) // first one, kept pending
	mustEvent(t, st, globex.ID, `{}`)
	if err := st.MarkDelivered(context.Background(), e2.ID, clock.now()); err != nil {
		t.Fatal(err)
	}

	bySub, err := st.ListEvents(context.Background(), ListFilter{SubscriptionID: acme.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(bySub) != 2 {
		t.Errorf("by subscription: got %d events, want 2", len(bySub))
	}

	byStatus, err := st.ListEvents(context.Background(), ListFilter{Status: StatusDelivered})
	if err != nil {
		t.Fatal(err)
	}
	if len(byStatus) != 1 || byStatus[0].ID != e2.ID {
		t.Errorf("by status: got %d/%v, want 1 (%s)", len(byStatus), byStatus, e2.ID)
	}

	byCustomer, err := st.ListEvents(context.Background(), ListFilter{CustomerID: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byCustomer) != 2 {
		t.Errorf("by customer: got %d events, want 2", len(byCustomer))
	}

	limited, err := st.ListEvents(context.Background(), ListFilter{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 {
		t.Errorf("limit: got %d events, want 1", len(limited))
	}
}
