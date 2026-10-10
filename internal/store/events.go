package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CreateEvent stores the event as pending with next_attempt_at = now. The row
// exists before the API answers 202 (durability requirement).
// Sequence is monotonic per customer so consumers can reorder deliveries.
func (s *Store) CreateEvent(ctx context.Context, subscriptionID string, payload json.RawMessage) (Event, error) {
	var ev Event
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var customerID string
		err := tx.QueryRowContext(ctx,
			`SELECT customer_id FROM subscriptions WHERE id = ?`, subscriptionID,
		).Scan(&customerID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("load subscription: %w", err)
		}

		var maxSeq sql.NullInt64
		if err := tx.QueryRowContext(ctx,
			`SELECT MAX(e.sequence) FROM events e
			   JOIN subscriptions s ON s.id = e.subscription_id
			  WHERE s.customer_id = ?`, customerID,
		).Scan(&maxSeq); err != nil {
			return fmt.Errorf("load next sequence: %w", err)
		}
		seq := int64(1)
		if maxSeq.Valid && maxSeq.Int64 >= seq {
			seq = maxSeq.Int64 + 1
		}

		now := s.now().UTC()
		ev = Event{
			ID:             newID("evt_"),
			SubscriptionID: subscriptionID,
			CustomerID:     customerID,
			Sequence:       seq,
			Payload:        payload,
			Status:         StatusPending,
			NextAttemptAt:  now,
			CreatedAt:      now,
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO events (id, subscription_id, sequence, payload, status,
				attempt_count, next_attempt_at, lease_until, last_error, created_at, delivered_at)
				VALUES (?, ?, ?, ?, ?, 0, ?, NULL, NULL, ?, NULL)`,
			ev.ID, ev.SubscriptionID, ev.Sequence, string(ev.Payload), ev.Status,
			formatTime(ev.NextAttemptAt), formatTime(ev.CreatedAt),
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		return nil
	})
	if err != nil {
		return Event{}, err
	}
	return ev, nil
}

// GetEvent returns the event with its full attempt history (FR7).
func (s *Store) GetEvent(ctx context.Context, id string) (Event, error) {
	ev, err := scanEvent(s.db.QueryRowContext(ctx,
		`SELECT `+eventColumns+` FROM events e JOIN subscriptions s ON s.id = e.subscription_id WHERE e.id = ?`, id))
	if err != nil {
		return Event{}, err
	}
	attempts, err := s.listAttempts(ctx, id)
	if err != nil {
		return Event{}, err
	}
	ev.Attempts = attempts
	return ev, nil
}

func (s *Store) listAttempts(ctx context.Context, eventID string) ([]Attempt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, event_id, attempt, attempted_at, http_status, error, duration_ms
		   FROM delivery_attempts WHERE event_id = ? ORDER BY id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	defer rows.Close()

	var out []Attempt
	for rows.Next() {
		var (
			a           Attempt
			attemptedAt string
			status      sql.NullInt64
		)
		if err := rows.Scan(&a.ID, &a.EventID, &a.Attempt, &attemptedAt, &status, &a.Error, &a.DurationMS); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		if a.AttemptedAt, err = parseTime(attemptedAt); err != nil {
			return nil, err
		}
		if status.Valid {
			s := int(status.Int64)
			a.HTTPStatus = &s
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate attempts: %w", err)
	}
	return out, nil
}

type ListFilter struct {
	SubscriptionID string
	CustomerID     string
	Status         Status
	Limit          int
}

// ListEvents supports FR8 (inspection listing). Conditions are appended as
// fixed SQL fragments with bound placeholders; user input never enters the SQL
// text.
func (s *Store) ListEvents(ctx context.Context, f ListFilter) ([]Event, error) {
	var (
		conds []string
		args  []any
	)
	if f.SubscriptionID != "" {
		conds = append(conds, "e.subscription_id = ?")
		args = append(args, f.SubscriptionID)
	}
	if f.CustomerID != "" {
		conds = append(conds, "s.customer_id = ?")
		args = append(args, f.CustomerID)
	}
	if f.Status != "" {
		conds = append(conds, "e.status = ?")
		args = append(args, f.Status)
	}
	query := `SELECT ` + eventColumns + ` FROM events e JOIN subscriptions s ON s.id = e.subscription_id`
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	query += " ORDER BY e.created_at DESC, e.id DESC LIMIT ?"
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

// RecordAttempt always writes a delivery_attempts row (status, error,
// duration) and bumps attempt_count. It does not change status or the lease:
// the caller decides delivered / retry / failed immediately afterwards.
func (s *Store) RecordAttempt(ctx context.Context, eventID string, a Attempt) error {
	var status any
	if a.HTTPStatus != nil {
		status = *a.HTTPStatus
	}
	err := s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO delivery_attempts (event_id, attempt, attempted_at, http_status, error, duration_ms)
				VALUES (?, ?, ?, ?, ?, ?)`,
			eventID, a.Attempt, formatTime(a.AttemptedAt), status, a.Error, a.DurationMS,
		); err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE events SET attempt_count = attempt_count + 1 WHERE id = ?`, eventID)
		if err != nil {
			return fmt.Errorf("bump attempt_count: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("rows affected: %w", err)
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
	return err
}

// MarkDelivered releases the lease and records the terminal success state
// (FR4). next_attempt_at is reset to the delivery time so a terminal event
// never advertises a future attempt.
func (s *Store) MarkDelivered(ctx context.Context, eventID string, at time.Time) error {
	return s.updateTerminal(ctx, eventID,
		`UPDATE events SET status = ?, delivered_at = ?, next_attempt_at = ?, lease_until = NULL WHERE id = ? AND status = ?`,
		StatusDelivered, formatTime(at), formatTime(at), eventID, StatusPending)
}

// MarkFailed moves the event to its terminal state (FR6). The row and its
// attempts are never deleted, so it can be replayed manually later.
func (s *Store) MarkFailed(ctx context.Context, eventID, reason string, at time.Time) error {
	return s.updateTerminal(ctx, eventID,
		`UPDATE events SET status = ?, last_error = ?, lease_until = NULL, next_attempt_at = ? WHERE id = ? AND status = ?`,
		StatusFailed, reason, formatTime(at), eventID, StatusPending)
}

// Reschedule puts the event back in the queue at nextAt and releases the lease,
// so no worker is parked between attempts. lastErr is recorded for FR7.
func (s *Store) Reschedule(ctx context.Context, eventID string, nextAt time.Time, lastErr string) error {
	err := s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE events SET next_attempt_at = ?, last_error = ?, lease_until = NULL
			  WHERE id = ? AND status = ?`,
			formatTime(nextAt), lastErr, eventID, StatusPending)
		if err != nil {
			return fmt.Errorf("reschedule event: %w", err)
		}
		return requireRow(res)
	})
	return err
}

func (s *Store) updateTerminal(ctx context.Context, eventID, query string, args ...any) error {
	err := s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("update event %s: %w", eventID, err)
		}
		return requireRow(res)
	})
	return err
}

func requireRow(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		// The event already left `pending` (e.g. another path finished it);
		// returning ErrNotFound lets the caller log it instead of double-applying.
		return fmt.Errorf("event not pending: %w", ErrNotFound)
	}
	return nil
}

// Counts powers the /healthz counters (pending, delivered, failed).
func (s *Store) Counts(ctx context.Context) (Counts, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM events GROUP BY status`)
	if err != nil {
		return Counts{}, fmt.Errorf("count events: %w", err)
	}
	defer rows.Close()

	var c Counts
	for rows.Next() {
		var (
			status Status
			n      int
		)
		if err := rows.Scan(&status, &n); err != nil {
			return Counts{}, fmt.Errorf("scan count: %w", err)
		}
		switch status {
		case StatusPending:
			c.Pending = n
		case StatusDelivered:
			c.Delivered = n
		case StatusFailed:
			c.Failed = n
		}
	}
	if err := rows.Err(); err != nil {
		return Counts{}, fmt.Errorf("iterate counts: %w", err)
	}
	return c, nil
}
