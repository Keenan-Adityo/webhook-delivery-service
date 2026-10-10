// Package api exposes the HTTP surface: event ingest, status queries and
// subscription registration. Validation always happens before anything is
// stored, so an invalid request leaves no rows behind (FR2).
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"webhook-delivery-service/internal/config"
	"webhook-delivery-service/internal/store"
)

// envelopeSlack leaves room for the JSON envelope around the payload, so the
// body limit rejects oversized payloads with 413 instead of a generic 400.
const envelopeSlack = 4 << 10

type Server struct {
	store *store.Store
	cfg   config.Config
	log   *slog.Logger
	now   func() time.Time
}

func New(st *store.Store, cfg config.Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: st, cfg: cfg, log: log, now: time.Now}
}

// Routes returns the mux for the whole API.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.handleCreateEvent)
	mux.HandleFunc("GET /events", s.handleListEvents)
	mux.HandleFunc("GET /events/{id}", s.handleGetEvent)
	mux.HandleFunc("POST /subscriptions", s.handleCreateSubscription)
	mux.HandleFunc("GET /subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return s.logRequests(mux)
}

func (s *Server) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SubscriptionID string          `json:"subscription_id"`
		Payload        json.RawMessage `json:"payload"`
	}
	if !decodeBody(w, r, s.cfg.MaxPayloadBytes+envelopeSlack, &req) {
		return
	}
	if req.SubscriptionID == "" {
		writeError(w, http.StatusBadRequest, "subscription_id is required")
		return
	}
	if len(req.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}
	if !json.Valid(req.Payload) {
		writeError(w, http.StatusBadRequest, "payload must be valid JSON")
		return
	}
	if int64(len(req.Payload)) > s.cfg.MaxPayloadBytes {
		writeError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("payload exceeds %d bytes", s.cfg.MaxPayloadBytes))
		return
	}

	// Unknown subscription is rejected before anything is stored.
	if _, err := s.store.GetSubscription(r.Context(), req.SubscriptionID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "unknown subscription_id")
			return
		}
		s.log.ErrorContext(r.Context(), "load subscription", "error", err, "subscription_id", req.SubscriptionID)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Persisted before the 202: durability first, delivery asynchronous.
	ev, err := s.store.CreateEvent(r.Context(), req.SubscriptionID, req.Payload)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "unknown subscription_id")
			return
		}
		s.log.ErrorContext(r.Context(), "create event", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"event_id": ev.ID,
		"status":   ev.Status,
	})
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	ev, err := s.store.GetEvent(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "event not found")
			return
		}
		s.log.ErrorContext(r.Context(), "get event", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.ListFilter{
		SubscriptionID: q.Get("subscription_id"),
		CustomerID:     q.Get("customer_id"),
	}
	if st := q.Get("status"); st != "" {
		switch store.Status(st) {
		case store.StatusPending, store.StatusDelivered, store.StatusFailed:
			filter.Status = store.Status(st)
		default:
			writeError(w, http.StatusBadRequest, "status must be one of pending, delivered, failed")
			return
		}
	}
	if lim := q.Get("limit"); lim != "" {
		n, err := strconv.Atoi(lim)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		filter.Limit = n
	}

	events, err := s.store.ListEvents(r.Context(), filter)
	if err != nil {
		s.log.ErrorContext(r.Context(), "list events", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if events == nil {
		events = []store.Event{} // keep [] instead of null in JSON
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID  string `json:"customer_id"`
		EndpointURL string `json:"endpoint_url"`
	}
	if !decodeBody(w, r, envelopeSlack, &req) {
		return
	}
	if req.CustomerID == "" {
		writeError(w, http.StatusBadRequest, "customer_id is required")
		return
	}
	if err := validateEndpointURL(req.EndpointURL); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	sub, err := s.store.CreateSubscription(r.Context(), req.CustomerID, req.EndpointURL)
	if err != nil {
		s.log.ErrorContext(r.Context(), "create subscription", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.Header().Set("Location", "/subscriptions/"+sub.ID)
	writeJSON(w, http.StatusCreated, sub)
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := s.store.ListSubscriptions(r.Context())
	if err != nil {
		s.log.ErrorContext(r.Context(), "list subscriptions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if subs == nil {
		subs = []store.Subscription{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": subs})
}

// handleHealth is the observability endpoint: liveness plus the counters the
// PRD asks for (pending, delivered, failed), read live from the datastore.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	counts, err := s.store.Counts(r.Context())
	if err != nil {
		s.log.ErrorContext(r.Context(), "healthz counts", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"pending":   counts.Pending,
		"delivered": counts.Delivered,
		"failed":    counts.Failed,
	})
}

func validateEndpointURL(raw string) error {
	if raw == "" {
		return errors.New("endpoint_url is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("endpoint_url must be an absolute URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("endpoint_url scheme must be http or https")
	}
	return nil
}

// decodeBody reads at most limit bytes, decodes JSON and turns failures into
// the right 4xx. It returns false if a response has already been written.
func decodeBody(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer r.Body.Close()

	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "malformed JSON body")
		}
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers are already sent; the only useful thing left is the log.
		slog.Default().Error("write response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
