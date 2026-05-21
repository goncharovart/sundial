package sundial

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fakePool returns a non-nil pool pointer suitable for use in unit tests
// where the database is never actually contacted. The pool is intentionally
// constructed via the zero value — calls into it would panic, which is the
// point: any test reaching the DB layer is wrong at this level.
//
// Integration tests live under a build tag and use testcontainers-go.
func fakePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return &pgxpool.Pool{}
}

func TestNew_Validation(t *testing.T) {
	t.Run("rejects nil pool", func(t *testing.T) {
		_, err := New(nil, Options{NodeID: "n1"})
		if err == nil {
			t.Fatal("expected error for nil pool")
		}
	})

	t.Run("rejects empty NodeID", func(t *testing.T) {
		_, err := New(fakePool(t), Options{})
		if !errors.Is(err, ErrEmptyNodeID) {
			t.Fatalf("expected ErrEmptyNodeID, got %v", err)
		}
	})

	t.Run("rejects too-small tick interval", func(t *testing.T) {
		_, err := New(fakePool(t), Options{NodeID: "n1", TickInterval: 50 * time.Millisecond})
		if !errors.Is(err, ErrTickTooSmall) {
			t.Fatalf("expected ErrTickTooSmall, got %v", err)
		}
	})

	t.Run("fills defaults", func(t *testing.T) {
		s, err := New(fakePool(t), Options{NodeID: "n1"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.opts.TickInterval != time.Second {
			t.Errorf("TickInterval default = %s, want 1s", s.opts.TickInterval)
		}
		if s.opts.ShutdownGrace != 30*time.Second {
			t.Errorf("ShutdownGrace default = %s, want 30s", s.opts.ShutdownGrace)
		}
		if s.opts.Schema != "public" {
			t.Errorf("Schema default = %q, want public", s.opts.Schema)
		}
	})
}

func TestSchedule_Validation(t *testing.T) {
	s, _ := New(fakePool(t), Options{NodeID: "n1"})
	every5s, _ := Every(5 * time.Second)
	noopHandler := HandlerFunc(func(context.Context) error { return nil })

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := s.Schedule("", every5s, noopHandler)
		if !errors.Is(err, ErrEmptyJobName) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("rejects nil schedule", func(t *testing.T) {
		_, err := s.Schedule("j", nil, noopHandler)
		if !errors.Is(err, ErrNilSchedule) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("rejects nil handler", func(t *testing.T) {
		_, err := s.Schedule("j", every5s, nil)
		if !errors.Is(err, ErrNilHandler) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("rejects duplicate job name", func(t *testing.T) {
		s2, _ := New(fakePool(t), Options{NodeID: "n1"})
		if _, err := s2.Schedule("dup", every5s, noopHandler); err != nil {
			t.Fatalf("first Schedule failed: %v", err)
		}
		_, err := s2.Schedule("dup", every5s, noopHandler)
		if !errors.Is(err, ErrJobNameTaken) {
			t.Fatalf("expected ErrJobNameTaken, got %v", err)
		}
	})

	t.Run("rejects after Stop", func(t *testing.T) {
		s3, _ := New(fakePool(t), Options{NodeID: "n1"})
		s3.Stop()
		_, err := s3.Schedule("j", every5s, noopHandler)
		if !errors.Is(err, ErrSchedulerStopped) {
			t.Fatalf("expected ErrSchedulerStopped, got %v", err)
		}
	})

	t.Run("applies options and defaults", func(t *testing.T) {
		s4, _ := New(fakePool(t), Options{NodeID: "n1"})
		j, err := s4.Schedule("j", every5s, noopHandler,
			WithTimeout(2*time.Second),
			WithLeaderOnly(),
			WithMissedFire(MissedFireSkip),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if j.Options.Timeout != 2*time.Second {
			t.Errorf("Timeout = %s, want 2s", j.Options.Timeout)
		}
		if !j.Options.LeaderOnly {
			t.Error("LeaderOnly not set")
		}
		if j.Options.MissedFire != MissedFireSkip {
			t.Errorf("MissedFire = %v, want MissedFireSkip", j.Options.MissedFire)
		}
		if j.Options.Retry.MaxAttempts != 3 {
			t.Errorf("Retry default not applied: MaxAttempts = %d", j.Options.Retry.MaxAttempts)
		}
	})
}

func TestJobs_SortedByName(t *testing.T) {
	s, _ := New(fakePool(t), Options{NodeID: "n1"})
	every1m, _ := Every(time.Minute)
	noop := HandlerFunc(func(context.Context) error { return nil })

	for _, name := range []string{"charlie", "alpha", "bravo"} {
		if _, err := s.Schedule(name, every1m, noop); err != nil {
			t.Fatalf("Schedule(%q) failed: %v", name, err)
		}
	}

	jobs := s.Jobs()
	if len(jobs) != 3 {
		t.Fatalf("got %d jobs, want 3", len(jobs))
	}
	wantOrder := []string{"alpha", "bravo", "charlie"}
	for i, j := range jobs {
		if j.Name != wantOrder[i] {
			t.Errorf("jobs[%d].Name = %q, want %q", i, j.Name, wantOrder[i])
		}
	}
}

func TestRun_ReturnsOnContextCancel(t *testing.T) {
	s, _ := New(fakePool(t), Options{NodeID: "n1"})
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	// Give the goroutine a moment to enter the loop, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of cancel")
	}
}
