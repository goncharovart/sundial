// Command hello demonstrates the Sundial public API.
//
// It does NOT execute jobs yet — the dispatcher loop lands in a follow-up
// commit. What this example shows is the call-site shape downstream code
// will use once the dispatcher is wired up. It registers three jobs with
// different schedules, prints the registry, then waits for Ctrl-C to
// exercise graceful shutdown.
//
// Run it:
//
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

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		logger.Error("connect to Postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	s, err := sundial.New(pool, sundial.Options{
		NodeID:       hostnameOr("hello-node"),
		TickInterval: time.Second,
	})
	if err != nil {
		logger.Error("create scheduler", "error", err)
		os.Exit(1)
	}

	hourly, _ := sundial.Cron("@hourly")
	every10s, _ := sundial.Every(10 * time.Second)
	tomorrow := sundial.At(time.Now().Add(24 * time.Hour))

	register(s, logger, "report-hourly", hourly, func(ctx context.Context) error {
		logger.Info("generating hourly report")
		return nil
	}, sundial.WithLeaderOnly())

	register(s, logger, "health-probe", every10s, func(ctx context.Context) error {
		logger.Info("probing dependencies")
		return nil
	})

	register(s, logger, "send-launch-email", tomorrow, func(ctx context.Context) error {
		logger.Info("sending launch email", "fire_time", time.Now())
		return nil
	}, sundial.WithMissedFire(sundial.MissedFireRunOnce))

	for _, j := range s.Jobs() {
		fmt.Printf("  registered: %-20s  %s  (kind=%s)\n", j.Name, j.Schedule.String(), j.Schedule.Kind())
	}

	logger.Info("scheduler ready (dispatcher loop arrives in a follow-up commit) — Ctrl-C to exit")
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

func hostnameOr(fallback string) string {
	h, err := os.Hostname()
	if err != nil {
		return fallback
	}
	return h
}
