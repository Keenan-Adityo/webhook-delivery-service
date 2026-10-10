// Package config holds every tunable of the service. Time-based values carry
// the PRD defaults, so tests can scale them down without touching code.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	// HTTP server.
	Addr string
	// SQLite file path.
	DBPath string
	// Optional JSON file with subscriptions seeded at startup (FR10).
	SeedFile string

	// Dispatcher.
	Workers        int
	PerCustomerCap int
	PollInterval   time.Duration
	// LeaseDuration must exceed RequestTimeout: a claim stays valid for the
	// whole in-flight request, otherwise a slow endpoint would be double-sent.
	LeaseDuration   time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
	// MinRetryGap is the shortest distance between two attempts of the same
	// event, so an aggressive Retry-After cannot become a hot loop.
	MinRetryGap time.Duration
	// MaxPayloadBytes is the size of the caller's "payload" value (FR2).
	MaxPayloadBytes int64

	Retry   Retry
	Breaker Breaker
}

type Retry struct {
	InitialDelay time.Duration
	Multiplier   float64
	// Jitter is the ± fraction applied to each delay (0.2 = ±20%).
	Jitter   float64
	MaxDelay time.Duration
	// Deadline is measured from event creation.
	Deadline time.Duration
}

type Breaker struct {
	Threshold   int
	CoolDown    time.Duration
	MaxCoolDown time.Duration
}

// Default returns the PRD defaults.
func Default() Config {
	return Config{
		Addr:            ":8080",
		DBPath:          "webhook.db",
		SeedFile:        "",
		Workers:         8,
		PerCustomerCap:  2,
		PollInterval:    250 * time.Millisecond,
		LeaseDuration:   30 * time.Second,
		RequestTimeout:  10 * time.Second,
		ShutdownTimeout: 15 * time.Second,
		MinRetryGap:     time.Second,
		MaxPayloadBytes: 1 << 20, // 1 MiB
		Retry: Retry{
			InitialDelay: 10 * time.Second,
			Multiplier:   2,
			Jitter:       0.2,
			MaxDelay:     30 * time.Minute,
			Deadline:     24 * time.Hour,
		},
		Breaker: Breaker{
			Threshold:   5,
			CoolDown:    30 * time.Second,
			MaxCoolDown: 10 * time.Minute,
		},
	}
}

// Load starts from the defaults and overrides them from the environment.
// Invalid values are an error rather than a silent fallback, so a typo in a
// deployment does not quietly change retry behaviour.
func Load() (Config, error) {
	c := Default()

	c.Addr = envStr("ADDR", c.Addr)
	c.DBPath = envStr("DB_PATH", c.DBPath)
	c.SeedFile = envStr("SEED_FILE", c.SeedFile)

	var err error
	if c.Workers, err = envInt("WORKERS", c.Workers); err != nil {
		return c, err
	}
	if c.PerCustomerCap, err = envInt("PER_CUSTOMER_CAP", c.PerCustomerCap); err != nil {
		return c, err
	}
	if c.PollInterval, err = envDur("POLL_INTERVAL", c.PollInterval); err != nil {
		return c, err
	}
	if c.LeaseDuration, err = envDur("LEASE_DURATION", c.LeaseDuration); err != nil {
		return c, err
	}
	if c.RequestTimeout, err = envDur("REQUEST_TIMEOUT", c.RequestTimeout); err != nil {
		return c, err
	}
	if c.ShutdownTimeout, err = envDur("SHUTDOWN_TIMEOUT", c.ShutdownTimeout); err != nil {
		return c, err
	}
	if c.MinRetryGap, err = envDur("MIN_RETRY_GAP", c.MinRetryGap); err != nil {
		return c, err
	}
	if c.MaxPayloadBytes, err = envInt64("MAX_PAYLOAD_BYTES", c.MaxPayloadBytes); err != nil {
		return c, err
	}

	if c.Retry.InitialDelay, err = envDur("RETRY_INITIAL_DELAY", c.Retry.InitialDelay); err != nil {
		return c, err
	}
	if c.Retry.Multiplier, err = envFloat("RETRY_MULTIPLIER", c.Retry.Multiplier); err != nil {
		return c, err
	}
	if c.Retry.Jitter, err = envFloat("RETRY_JITTER", c.Retry.Jitter); err != nil {
		return c, err
	}
	if c.Retry.MaxDelay, err = envDur("RETRY_MAX_DELAY", c.Retry.MaxDelay); err != nil {
		return c, err
	}
	if c.Retry.Deadline, err = envDur("RETRY_DEADLINE", c.Retry.Deadline); err != nil {
		return c, err
	}

	if c.Breaker.Threshold, err = envInt("BREAKER_THRESHOLD", c.Breaker.Threshold); err != nil {
		return c, err
	}
	if c.Breaker.CoolDown, err = envDur("BREAKER_COOLDOWN", c.Breaker.CoolDown); err != nil {
		return c, err
	}
	if c.Breaker.MaxCoolDown, err = envDur("BREAKER_MAX_COOLDOWN", c.Breaker.MaxCoolDown); err != nil {
		return c, err
	}

	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func (c Config) Validate() error {
	switch {
	case c.Workers < 1:
		return fmt.Errorf("WORKERS must be >= 1, got %d", c.Workers)
	case c.PerCustomerCap < 1:
		return fmt.Errorf("PER_CUSTOMER_CAP must be >= 1, got %d", c.PerCustomerCap)
	case c.PollInterval <= 0:
		return fmt.Errorf("POLL_INTERVAL must be > 0, got %s", c.PollInterval)
	case c.MinRetryGap <= 0:
		return fmt.Errorf("MIN_RETRY_GAP must be > 0, got %s", c.MinRetryGap)
	case c.RequestTimeout <= 0:
		return fmt.Errorf("REQUEST_TIMEOUT must be > 0, got %s", c.RequestTimeout)
	case c.LeaseDuration <= c.RequestTimeout:
		return fmt.Errorf("LEASE_DURATION (%s) must be greater than REQUEST_TIMEOUT (%s)", c.LeaseDuration, c.RequestTimeout)
	case c.MaxPayloadBytes < 1:
		return fmt.Errorf("MAX_PAYLOAD_BYTES must be >= 1, got %d", c.MaxPayloadBytes)
	case c.Retry.Multiplier < 1:
		return fmt.Errorf("RETRY_MULTIPLIER must be >= 1, got %g", c.Retry.Multiplier)
	case c.Retry.Jitter < 0 || c.Retry.Jitter >= 1:
		return fmt.Errorf("RETRY_JITTER must be in [0,1), got %g", c.Retry.Jitter)
	case c.Retry.InitialDelay <= 0:
		return fmt.Errorf("RETRY_INITIAL_DELAY must be > 0, got %s", c.Retry.InitialDelay)
	case c.Retry.MaxDelay < c.Retry.InitialDelay:
		return fmt.Errorf("RETRY_MAX_DELAY (%s) must be >= RETRY_INITIAL_DELAY (%s)", c.Retry.MaxDelay, c.Retry.InitialDelay)
	case c.Retry.Deadline <= 0:
		return fmt.Errorf("RETRY_DEADLINE must be > 0, got %s", c.Retry.Deadline)
	case c.Breaker.Threshold < 1:
		return fmt.Errorf("BREAKER_THRESHOLD must be >= 1, got %d", c.Breaker.Threshold)
	case c.Breaker.CoolDown <= 0:
		return fmt.Errorf("BREAKER_COOLDOWN must be > 0, got %s", c.Breaker.CoolDown)
	case c.Breaker.MaxCoolDown < c.Breaker.CoolDown:
		return fmt.Errorf("BREAKER_MAX_COOLDOWN (%s) must be >= BREAKER_COOLDOWN (%s)", c.Breaker.MaxCoolDown, c.Breaker.CoolDown)
	}
	return nil
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: parse %q: %w", key, v, err)
	}
	return n, nil
}

func envInt64(key string, def int64) (int64, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: parse %q: %w", key, v, err)
	}
	return n, nil
}

func envFloat(key string, def float64) (float64, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: parse %q: %w", key, v, err)
	}
	return f, nil
}

func envDur(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: parse %q: %w", key, v, err)
	}
	return d, nil
}
