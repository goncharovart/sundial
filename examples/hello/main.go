// Command hello demonstrates the Sundial public API end-to-end.
//
// It registers three jobs with different schedules and runs the
// dispatcher until Ctrl-C. By default it uses an in-memory backend so
// the example needs no infrastructure. Pass DATABASE_URL to use a real
// Postgres instance instead.
//
// Run it:
//
//	# in-memory (zero setup)
//	go run ./examples/hello
//
//	# Postgres
//	export DATABASE_URL=postgres://sundial:sundial@localhost:5432/sundial?sslmode=disable
//	go run ./examples/hello
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goncharovart/sundial"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opts := sundial.Options{
		NodeID:       sundial.AutoNodeID(),
		TickInterval: time.Second,
	}

	var schedOpts []sundial.SchedulerOption
	var pool *pgxpool.Pool

	if url := os.Getenv("DATABASE_URL"); url != "" {
		var err error
		pool, err = pgxpool.New(ctx, url)
		if err != nil {
			logger.Error("connect to Postgres", "error", err)
			os.Exit(1)
		}
		defer pool.Close()
		logger.Info("using Postgres backend", "url", redacted(url))
	} else {
		schedOpts = append(schedOpts, sundial.WithStorage(sundial.NewMemoryStorage()))
		logger.Info("using in-memory backend (set DATABASE_URL for Postgres)")
	}
	schedOpts = append(schedOpts, sundial.WithLogger(logger))

	s, err := sundial.New(pool, opts, schedOpts...)
	if err != nil {
		logger.Error("create scheduler", "error", err)
		os.Exit(1)
	}

	every5s, _ := sundial.Every(5 * time.Second)
	every12s, _ := sundial.Every(12 * time.Second)
	tomorrow := sundial.At(time.Now().Add(24 * time.Hour))

	var hourlyHits, probeHits int

	register(s, logger, "report-every-12s", every12s, func(_ context.Context) error {
		hourlyHits++
		logger.Info("report fired", "total", hourlyHits)
		return nil
	}, sundial.WithLeaderOnly())

	register(s, logger, "health-probe-5s", every5s, func(_ context.Context) error {
		probeHits++
		logger.Info("health probe", "total", probeHits)
		return nil
	})

	register(s, logger, "send-launch-email", tomorrow, func(_ context.Context) error {
		logger.Info("would send launch email")
		return nil
	}, sundial.WithMissedFire(sundial.MissedFireRunOnce))

	for _, j := range s.Jobs() {
		fmt.Printf("  registered: %-22s  %s  (kind=%s)\n", j.Name, j.Schedule.String(), j.Schedule.Kind())
	}

	logger.Info("scheduler running — Ctrl-C to exit cleanly")
	if err := s.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("scheduler exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("clean shutdown")
}

func register(
	s *sundial.Scheduler,
	logger *slog.Logger,
	name string,
	sch sundial.Schedule,
	fn sundial.HandlerFunc,
	opts ...sundial.JobOption,
) {
	if _, err := s.Schedule(name, sch, fn, opts...); err != nil {
		logger.Error("Schedule", "name", name, "error", err)
		os.Exit(1)
	}
}

// hostnameOr remains in the example as a tiny "do it yourself"
// reference — sundial.AutoNodeID() does the same thing with a
// stronger fallback chain. Kept here so contributors can see both
// shapes side-by-side.
func hostnameOr(fallback string) string {
	h, err := os.Hostname()
	if err != nil {
		return fallback
	}
	return h
}

func redacted(url string) string {
	// Strip credentials from the connection string for log output.
	at := -1
	for i := 0; i < len(url); i++ {
		if url[i] == '@' {
			at = i
		}
	}
	if at < 0 {
		return url
	}
	// keep scheme prefix
	scheme := ""
	for i := 0; i < len(url)-2; i++ {
		if url[i] == ':' && url[i+1] == '/' && url[i+2] == '/' {
			scheme = url[:i+3]
			break
		}
	}
	return scheme + "***" + url[at:]
}
