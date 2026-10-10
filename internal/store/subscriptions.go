package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CreateSubscription registers a customer endpoint (FR10).
func (s *Store) CreateSubscription(ctx context.Context, customerID, endpointURL string) (Subscription, error) {
	sub := Subscription{
		ID:          newID("sub_"),
		CustomerID:  customerID,
		EndpointURL: endpointURL,
		CreatedAt:   s.now().UTC(),
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO subscriptions (id, customer_id, endpoint_url, created_at) VALUES (?, ?, ?, ?)`,
		sub.ID, sub.CustomerID, sub.EndpointURL, formatTime(sub.CreatedAt),
	)
	if err != nil {
		return Subscription{}, fmt.Errorf("insert subscription: %w", err)
	}
	return sub, nil
}

func (s *Store) GetSubscription(ctx context.Context, id string) (Subscription, error) {
	var (
		sub       Subscription
		createdAt string
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT id, customer_id, endpoint_url, created_at FROM subscriptions WHERE id = ?`, id,
	).Scan(&sub.ID, &sub.CustomerID, &sub.EndpointURL, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, fmt.Errorf("get subscription: %w", err)
	}
	if sub.CreatedAt, err = parseTime(createdAt); err != nil {
		return Subscription{}, err
	}
	return sub, nil
}

func (s *Store) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, customer_id, endpoint_url, created_at FROM subscriptions ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()

	var out []Subscription
	for rows.Next() {
		var (
			sub       Subscription
			createdAt string
		)
		if err := rows.Scan(&sub.ID, &sub.CustomerID, &sub.EndpointURL, &createdAt); err != nil {
			return nil, fmt.Errorf("scan subscription: %w", err)
		}
		if sub.CreatedAt, err = parseTime(createdAt); err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate subscriptions: %w", err)
	}
	return out, nil
}
