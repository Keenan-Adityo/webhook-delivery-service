// cmd/server runs the webhook delivery service: one process, one command.
//
//	go run ./cmd/server
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webhook-delivery-service/internal/api"
	"webhook-delivery-service/internal/breaker"
	"webhook-delivery-service/internal/config"
	"webhook-delivery-service/internal/delivery"
	"webhook-delivery-service/internal/dispatcher"
	"webhook-delivery-service/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	st, err := store.Open(cfg.DBPath, nil)
	if err != nil {
		log.Error("open store", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	if cfg.SeedFile != "" {
		n, err := seedSubscriptions(context.Background(), st, cfg.SeedFile)
		if err != nil {
			log.Error("seed subscriptions", "error", err, "file", cfg.SeedFile)
			os.Exit(1)
		}
		log.Info("seeded subscriptions", "file", cfg.SeedFile, "created", n)
	}
	subs, err := st.ListSubscriptions(context.Background())
	if err != nil {
		log.Error("list subscriptions", "error", err)
		os.Exit(1)
	}
	for _, s := range subs {
		log.Info("subscription", "id", s.ID, "customer_id", s.CustomerID, "endpoint_url", s.EndpointURL)
	}

	brk := breaker.New(cfg.Breaker.Threshold, cfg.Breaker.CoolDown, cfg.Breaker.MaxCoolDown)
	disp := dispatcher.New(cfg, st, delivery.NewSender(cfg.RequestTimeout, nil), brk, log, nil, nil)
	apiSrv := api.New(st, cfg, log)

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           apiSrv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// signalCtx fires on Ctrl-C / SIGTERM.
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Dispatcher context is cancelled separately so shutdown can stop claiming
	// while the HTTP server is still draining.
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	workersDone := make(chan struct{})
	go func() {
		disp.Run(workerCtx)
		close(workersDone)
	}()

	serveErr := make(chan error, 1)
	go func() {
		log.Info("server listening", "addr", cfg.Addr, "db", cfg.DBPath, "workers", cfg.Workers)
		serveErr <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			cancelWorkers()
			<-workersDone
			os.Exit(1)
		}
	case <-signalCtx.Done():
		log.Info("shutdown signal received")
	}

	// Graceful shutdown: stop claiming, let in-flight deliveries finish within
	// the timeout, then exit. Anything still leased is retried after restart.
	cancelWorkers()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}
	select {
	case <-workersDone:
		log.Info("shutdown complete")
	case <-shutdownCtx.Done():
		log.Warn("shutdown timeout: exiting with deliveries still in flight; leases will expire and the events will be retried after restart")
		os.Exit(1)
	}
}

// seedSubscriptions loads subscriptions from a JSON file (FR10) and skips
// entries that already exist, so restarting the process does not duplicate them.
func seedSubscriptions(ctx context.Context, st *store.Store, path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read seed file: %w", err)
	}
	var seeds []struct {
		CustomerID  string `json:"customer_id"`
		EndpointURL string `json:"endpoint_url"`
	}
	if err := json.Unmarshal(raw, &seeds); err != nil {
		return 0, fmt.Errorf("parse seed file: %w", err)
	}

	existing, err := st.ListSubscriptions(ctx)
	if err != nil {
		return 0, fmt.Errorf("list subscriptions: %w", err)
	}
	seen := make(map[string]bool, len(existing))
	for _, s := range existing {
		seen[s.CustomerID+"\x00"+s.EndpointURL] = true
	}

	created := 0
	for _, s := range seeds {
		if s.CustomerID == "" || s.EndpointURL == "" {
			return created, fmt.Errorf("seed entry must have customer_id and endpoint_url: %+v", s)
		}
		if seen[s.CustomerID+"\x00"+s.EndpointURL] {
			continue
		}
		if _, err := st.CreateSubscription(ctx, s.CustomerID, s.EndpointURL); err != nil {
			return created, fmt.Errorf("create subscription for %s: %w", s.CustomerID, err)
		}
		created++
	}
	return created, nil
}
