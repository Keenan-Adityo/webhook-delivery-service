package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"webhook-delivery-service/internal/config"
	"webhook-delivery-service/internal/store"
)

type testAPI struct {
	srv   *httptest.Server
	store *store.Store
	cfg   config.Config
}

func newTestAPI(t *testing.T) *testAPI {
	t.Helper()
	cfg := config.Default()
	cfg.MaxPayloadBytes = 1024 // small, so the size limit is easy to exceed

	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"), nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(New(st, cfg, log).Routes())
	t.Cleanup(func() {
		srv.Close()
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return &testAPI{srv: srv, store: st, cfg: cfg}
}

func (a *testAPI) do(t *testing.T, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, a.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, raw
}

func (a *testAPI) mustHaveNoEvents(t *testing.T) {
	t.Helper()
	counts, err := a.store.Counts(t.Context())
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts != (store.Counts{}) {
		t.Errorf("invalid input must store nothing, got %+v", counts)
	}
}

func TestCreateEventRejectsInvalidInput(t *testing.T) {
	a := newTestAPI(t)
	validSub, raw := a.do(t, http.MethodPost, "/subscriptions",
		`{"customer_id":"acme","endpoint_url":"http://localhost:9090/hook"}`)
	if validSub.StatusCode != http.StatusCreated {
		t.Fatalf("register subscription: %d %s", validSub.StatusCode, raw)
	}
	var sub store.Subscription
	if err := json.Unmarshal(raw, &sub); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}

	big := `{"subscription_id":"` + sub.ID + `","payload":{"pad":"` +
		strings.Repeat("x", 2000) + `"}}`

	cases := []struct {
		name string
		body string
		want int
	}{
		{"malformed JSON", `{"subscription_id":`, http.StatusBadRequest},
		{"missing subscription_id", `{"payload":{}}`, http.StatusBadRequest},
		{"missing payload", `{"subscription_id":"` + sub.ID + `"}`, http.StatusBadRequest},
		{"payload contains invalid JSON", `{"subscription_id":"` + sub.ID + `","payload":{"a":}}`, http.StatusBadRequest},
		{"unknown subscription", `{"subscription_id":"sub_nope","payload":{}}`, http.StatusNotFound},
		{"oversized payload", big, http.StatusRequestEntityTooLarge},
		{"empty body", ``, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, raw := a.do(t, http.MethodPost, "/events", tc.body)
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d (%s)", resp.StatusCode, tc.want, raw)
			}
			a.mustHaveNoEvents(t)
		})
	}
}

func TestCreateEventAcceptedAndQueryable(t *testing.T) {
	a := newTestAPI(t)

	resp, raw := a.do(t, http.MethodPost, "/subscriptions",
		`{"customer_id":"acme","endpoint_url":"http://localhost:9090/hook"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register subscription: %d %s", resp.StatusCode, raw)
	}
	var sub store.Subscription
	if err := json.Unmarshal(raw, &sub); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}

	resp, raw = a.do(t, http.MethodPost, "/events",
		`{"subscription_id":"`+sub.ID+`","payload":{"hello":"world"}}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create event: %d %s", resp.StatusCode, raw)
	}
	var created struct {
		EventID string `json:"event_id"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode 202 body: %v", err)
	}
	if created.EventID == "" || created.Status != "pending" {
		t.Fatalf("202 body = %s, want event_id and status pending", raw)
	}

	// FR7: status endpoint exposes status, attempts, next attempt, last error.
	resp, raw = a.do(t, http.MethodGet, "/events/"+created.EventID, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get event: %d %s", resp.StatusCode, raw)
	}
	var ev store.Event
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if ev.Status != store.StatusPending || ev.AttemptCount != 0 {
		t.Errorf("status=%s attempt_count=%d, want pending/0", ev.Status, ev.AttemptCount)
	}
	if ev.NextAttemptAt.IsZero() || ev.CreatedAt.IsZero() {
		t.Errorf("missing timestamps: %+v", ev)
	}
	if ev.Sequence != 1 {
		t.Errorf("sequence = %d, want 1", ev.Sequence)
	}

	// FR8: listing with filters.
	resp, raw = a.do(t, http.MethodGet, "/events?status=pending", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list events: %d %s", resp.StatusCode, raw)
	}
	var list struct {
		Events []store.Event `json:"events"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Events) != 1 || list.Events[0].ID != created.EventID {
		t.Fatalf("pending list = %d events, want the created one", len(list.Events))
	}

	if resp, raw := a.do(t, http.MethodGet, "/events?status=bogus", ""); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid status filter: %d %s, want 400", resp.StatusCode, raw)
	}

	// Observability counters.
	resp, raw = a.do(t, http.MethodGet, "/healthz", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %d %s", resp.StatusCode, raw)
	}
	var health struct {
		Pending   int `json:"pending"`
		Delivered int `json:"delivered"`
		Failed    int `json:"failed"`
	}
	if err := json.Unmarshal(raw, &health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health.Pending != 1 || health.Delivered != 0 || health.Failed != 0 {
		t.Errorf("health counters = %+v, want 1 pending", health)
	}

	// Unknown event is a 404, not a 500.
	if resp, _ := a.do(t, http.MethodGet, "/events/evt_missing", ""); resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing event: got %d, want 404", resp.StatusCode)
	}
}

func TestSubscriptionValidation(t *testing.T) {
	a := newTestAPI(t)

	cases := []struct {
		name string
		body string
		want int
	}{
		{"missing customer", `{"endpoint_url":"http://localhost:9090"}`, http.StatusBadRequest},
		{"missing url", `{"customer_id":"acme"}`, http.StatusBadRequest},
		{"bad scheme", `{"customer_id":"acme","endpoint_url":"ftp://x/y"}`, http.StatusBadRequest},
		{"relative url", `{"customer_id":"acme","endpoint_url":"/hook"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, raw := a.do(t, http.MethodPost, "/subscriptions", tc.body)
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d (%s)", resp.StatusCode, tc.want, raw)
			}
		})
	}

	resp, raw := a.do(t, http.MethodGet, "/subscriptions", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list subscriptions: %d %s", resp.StatusCode, raw)
	}
	var list struct {
		Subscriptions []store.Subscription `json:"subscriptions"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Subscriptions) != 0 {
		t.Errorf("got %d subscriptions, want 0", len(list.Subscriptions))
	}
}
