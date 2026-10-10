package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// claimSQL picks one row in a single statement so two workers can never take
// the same event. The per-customer cap counts that customer's events currently
// holding a live lease; when the cap is reached no more of that customer's
// events are claimable, which is what keeps one slow customer from occupying
// the whole pool. The UPDATE that sets the lease runs in the same transaction.
const claimSQL = `
SELECT ` + eventColumns + `
  FROM events e
  JOIN subscriptions s ON s.id = e.subscription_id
 WHERE e.status = ?
   AND e.next_attempt_at <= ?
   AND (e.lease_until IS NULL OR e.lease_until <= ?)
   AND (
       SELECT COUNT(*)
         FROM events x
         JOIN subscriptions s2 ON s2.id = x.subscription_id
        WHERE s2.customer_id = s.customer_id
          AND x.status = ?
          AND x.lease_until IS NOT NULL
          AND x.lease_until > ?
   ) < ?
 ORDER BY e.next_attempt_at ASC, e.created_at ASC
 LIMIT 1`

// ClaimDue atomically claims the oldest due event and stamps lease_until.
// An expired lease (crash mid-delivery) is simply claimable again, which is
// what makes the service crash-safe under at-least-once semantics.
func (s *Store) ClaimDue(ctx context.Context, now, leaseUntil time.Time, perCustomerCap int) (Event, bool, error) {
	var (
		ev    Event
		found bool
	)
	err := s.tx(ctx, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, claimSQL,
			StatusPending, formatTime(now), formatTime(now),
			StatusPending, formatTime(now), perCustomerCap,
		)
		e, err := scanEvent(row)
		if errors.Is(err, ErrNotFound) {
			return nil // nothing due
		}
		if err != nil {
			return fmt.Errorf("select due event: %w", err)
		}

		res, err := tx.ExecContext(ctx,
			`UPDATE events SET lease_until = ? WHERE id = ? AND status = ?`,
			formatTime(leaseUntil), e.ID, StatusPending)
		if err != nil {
			return fmt.Errorf("set lease: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("rows affected: %w", err)
		}
		if n == 0 {
			// Unreachable inside a single-writer transaction; guard anyway so
			// a failed lease never looks like a successful claim.
			return fmt.Errorf("lease update matched no row")
		}
		// The row was scanned before the UPDATE, so reflect the fresh lease in
		// the returned value.
		leased := leaseUntil.UTC()
		e.LeaseUntil = &leased
		ev, found = e, true
		return nil
	})
	if err != nil {
		return Event{}, false, err
	}
	return ev, found, nil
}
