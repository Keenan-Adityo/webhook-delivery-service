// Package store is the source of truth: subscriptions, events and delivery
// attempts in SQLite, with transactional claims for the dispatcher.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver: zero-setup, no cgo
)

// ErrNotFound is returned when a row does not exist; the API maps it to 404.
var ErrNotFound = errors.New("not found")

// timeLayout is fixed width so timestamps sort lexicographically in SQL.
// time.RFC3339Nano is not used because it drops trailing zeros, which breaks
// string ordering ("…:00.1Z" > "…:00.12Z").
const timeLayout = "2006-01-02T15:04:05.000000Z07:00"

const schema = `
CREATE TABLE IF NOT EXISTS subscriptions (
	id           TEXT PRIMARY KEY,
	customer_id  TEXT NOT NULL,
	endpoint_url TEXT NOT NULL,
	created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
	id              TEXT PRIMARY KEY,
	subscription_id TEXT NOT NULL REFERENCES subscriptions(id),
	sequence        INTEGER NOT NULL,
	payload         TEXT NOT NULL,
	status          TEXT NOT NULL,
	attempt_count   INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	lease_until     TEXT,
	last_error      TEXT,
	created_at      TEXT NOT NULL,
	delivered_at    TEXT
);

-- Serves the dispatcher query: due events, oldest first.
CREATE INDEX IF NOT EXISTS idx_events_status_next ON events(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS delivery_attempts (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id     TEXT NOT NULL REFERENCES events(id),
	attempt      INTEGER NOT NULL,
	attempted_at TEXT NOT NULL,
	http_status  INTEGER,
	error        TEXT,
	duration_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attempts_event ON delivery_attempts(event_id);
`

type Status string

const (
	StatusPending   Status = "pending"
	StatusDelivered Status = "delivered"
	StatusFailed    Status = "failed"
)

type Subscription struct {
	ID          string    `json:"id"`
	CustomerID  string    `json:"customer_id"`
	EndpointURL string    `json:"endpoint_url"`
	CreatedAt   time.Time `json:"created_at"`
}

type Event struct {
	ID             string `json:"event_id"`
	SubscriptionID string `json:"subscription_id"`
	// CustomerID and EndpointURL come from the subscription join; the worker
	// needs them on every claim and a second round-trip is not worth it.
	CustomerID    string          `json:"customer_id,omitempty"`
	EndpointURL   string          `json:"endpoint_url,omitempty"`
	Sequence      int64           `json:"sequence"`
	Payload       json.RawMessage `json:"payload"`
	Status        Status          `json:"status"`
	AttemptCount  int             `json:"attempt_count"`
	NextAttemptAt time.Time       `json:"next_attempt_at"`
	LeaseUntil    *time.Time      `json:"lease_until,omitempty"`
	LastError     string          `json:"last_error,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	DeliveredAt   *time.Time      `json:"delivered_at,omitempty"`
	Attempts      []Attempt       `json:"attempts,omitempty"`
}

type Attempt struct {
	ID          int64     `json:"id"`
	EventID     string    `json:"event_id"`
	Attempt     int       `json:"attempt"`
	AttemptedAt time.Time `json:"attempted_at"`
	HTTPStatus  *int      `json:"http_status,omitempty"`
	Error       string    `json:"error,omitempty"`
	DurationMS  int64     `json:"duration_ms"`
}

type Counts struct {
	Pending   int `json:"pending"`
	Delivered int `json:"delivered"`
	Failed    int `json:"failed"`
}

type Store struct {
	db  *sql.DB
	now func() time.Time
}

// Open opens (or creates) the database, runs migrations and returns a Store.
// now is the injected clock; nil means time.Now.
func Open(dbPath string, now func() time.Time) (*Store, error) {
	if now == nil {
		now = time.Now
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One connection serializes all access: transactions never contend, so a
	// claim (SELECT + UPDATE) is atomic without busy-wait/retry code. This is
	// safe because the service is a single process (a PRD non-goal is
	// multi-instance operation). Queries never hold the connection during
	// network I/O, so this does not stall the API or the workers.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{db: db, now: now}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for tests only.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return fmt.Errorf("%v (rollback: %w)", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the platform is broken; there is no safe
		// fallback for an id that must be unique.
		panic(fmt.Sprintf("generate id: %v", err))
	}
	return prefix + hex.EncodeToString(b[:])
}

func formatTime(t time.Time) string { return t.UTC().Format(timeLayout) }

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(timeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time %q: %w", s, err)
	}
	return t, nil
}

// scanEvent reads the 12 event columns produced by the claim/get queries.
func scanEvent(row interface{ Scan(...any) error }) (Event, error) {
	var (
		ev          Event
		payload     string
		nextAttempt string
		createdAt   string
		leaseUntil  sql.NullString
		deliveredAt sql.NullString
		lastError   sql.NullString
	)
	err := row.Scan(
		&ev.ID, &ev.SubscriptionID, &ev.Sequence, &payload, &ev.Status,
		&ev.AttemptCount, &nextAttempt, &leaseUntil, &lastError, &createdAt,
		&deliveredAt, &ev.CustomerID, &ev.EndpointURL,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, fmt.Errorf("scan event: %w", err)
	}
	if ev.NextAttemptAt, err = parseTime(nextAttempt); err != nil {
		return Event{}, err
	}
	if ev.CreatedAt, err = parseTime(createdAt); err != nil {
		return Event{}, err
	}
	ev.Payload = json.RawMessage(payload)
	ev.Status = Status(ev.Status)
	if leaseUntil.Valid && leaseUntil.String != "" {
		t, err := parseTime(leaseUntil.String)
		if err != nil {
			return Event{}, err
		}
		ev.LeaseUntil = &t
	}
	if deliveredAt.Valid && deliveredAt.String != "" {
		t, err := parseTime(deliveredAt.String)
		if err != nil {
			return Event{}, err
		}
		ev.DeliveredAt = &t
	}
	if lastError.Valid {
		ev.LastError = lastError.String
	}
	return ev, nil
}

const eventColumns = `e.id, e.subscription_id, e.sequence, e.payload, e.status,
	e.attempt_count, e.next_attempt_at, e.lease_until, e.last_error, e.created_at,
	e.delivered_at, s.customer_id, s.endpoint_url`
