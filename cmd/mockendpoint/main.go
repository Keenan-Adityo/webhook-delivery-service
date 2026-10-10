// cmd/mockendpoint is a small fake customer endpoint with switchable
// behaviours (PRD section 9): ok, fail, slow, flaky, down.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"webhook-delivery-service/internal/mockendpoint"
)

func main() {
	var (
		addr      = flag.String("addr", ":9090", "listen address")
		mode      = flag.String("mode", string(mockendpoint.ModeOK), "behaviour: ok | fail | slow | flaky | down")
		delay     = flag.Duration("delay", 15*time.Second, "sleep before answering (mode=slow)")
		failCount = flag.Int("fail-count", 3, "number of leading failures (mode=flaky)")
		downFor   = flag.Duration("down-for", 2*time.Minute, "how long the endpoint stays down (mode=down)")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	h := mockendpoint.New(mockendpoint.Options{
		Mode:      mockendpoint.Mode(*mode),
		Delay:     *delay,
		FailCount: *failCount,
		DownFor:   *downFor,
	})

	log.Info("mock endpoint listening",
		"addr", *addr, "mode", *mode, "delay", *delay,
		"fail_count", *failCount, "down_for", *downFor)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           logEach(log, h),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("serve", "error", err)
		os.Exit(1)
	}
}

// logEach logs one structured line per received delivery.
func logEach(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		before := time.Now()
		next.ServeHTTP(w, r)
		log.Info("received delivery",
			"event_id", r.Header.Get("X-Event-Id"),
			"attempt", r.Header.Get("X-Delivery-Attempt"),
			"path", r.URL.Path,
			"duration", time.Since(before),
		)
	})
}
