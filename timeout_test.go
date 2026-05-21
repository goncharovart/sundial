package sundial

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestJobTimeout_CancelsHandler — a handler that blocks forever on
// ctx.Done() must be cancelled when JobOptions.Timeout elapses. The
// resulting RunRecord should report RunFailed with a
// context.DeadlineExceeded-flavoured error.
func TestJobTimeout_CancelsHandler(t *testing.T) {
	mem := NewMemoryStorage()
	s, err := New(nil, Options{
		NodeID:        "timeout-node",
		TickInterval:  100 * time.Millisecond,
		ShutdownGrace: 500 * time.Millisecond,
	}, WithStorage(mem))
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var mu sync.Mutex
	var ctxErr error

	every, _ := Every(time.Second)
	job, err := s.Schedule("sleeper", every,
		func(ctx context.Context) error {
			// Block on ctx.Done(); record what cancellation we got.
			<-ctx.Done()
			mu.Lock()
			ctxErr = ctx.Err()
			mu.Unlock()
			close(done)
			return ctx.Err()
		},
		WithTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Seed a fire that is already due so the dispatcher picks it
	// up on the first tick.
	if _, err := mem.EnsureJob(context.Background(), job, time.Now().Add(-100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_ = s.Run(runCtx)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler never observed cancellation within the test budget")
	}

	mu.Lock()
	got := ctxErr
	mu.Unlock()
	if got != context.DeadlineExceeded {
		t.Fatalf("handler ctx.Err() = %v, want context.DeadlineExceeded", got)
	}

	runs := mem.Runs()
	if len(runs) == 0 {
		t.Fatal("no run records — dispatcher never claimed the job")
	}
	// We registered exactly one job — every run record in this test
	// belongs to it. Find the run that recorded the timeout failure.
	var run RunRecord
	var found bool
	for _, r := range runs {
		if r.Outcome == RunFailed && strings.Contains(r.Error, "context deadline exceeded") {
			run = r
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no RunFailed record with deadline-exceeded error in %+v", runs)
	}

	// The recorded duration should be within the timeout window
	// plus a small allowance for scheduler/cancellation latency.
	dur := run.FinishedAt.Sub(run.StartedAt)
	if dur < 150*time.Millisecond {
		t.Errorf("recorded duration %s shorter than the 200ms timeout (clock skew?)", dur)
	}
	if dur > 400*time.Millisecond {
		t.Errorf("recorded duration %s is longer than timeout + 200ms cancellation allowance", dur)
	}
	_ = job // job ID is private; we matched the run by outcome+error instead
}
