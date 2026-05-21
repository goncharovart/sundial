package sundial

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestComputeBackoff_GrowsExponentially(t *testing.T) {
	p := RetryPolicy{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    5,
	}
	// Without jitter the deterministic curve is 100ms, 200ms, 400ms,
	// 800ms, 1.6s. Jitter expands each by [0.5, 1.5). Assert min/max
	// envelopes hold over enough samples to be statistically safe.
	for attempt := 1; attempt <= 4; attempt++ {
		base := float64(p.InitialBackoff) * pow2(attempt-1)
		minD := time.Duration(base * 0.5)
		maxD := time.Duration(base * 1.5)
		for i := 0; i < 32; i++ {
			got := computeBackoff(p, attempt)
			if got < minD || got > maxD {
				t.Errorf("attempt=%d got=%s, expected in [%s, %s]", attempt, got, minD, maxD)
			}
		}
	}
}

func TestComputeBackoff_ClampsToMax(t *testing.T) {
	p := RetryPolicy{
		InitialBackoff: time.Second,
		MaxBackoff:     2 * time.Second,
		Multiplier:     10.0,
		MaxAttempts:    5,
	}
	// attempt=3 would be 1s × 100 = 100s if unclamped. Clamp + jitter:
	// max is 2s × 1.5 = 3s. Run many samples; none should exceed 3s.
	for i := 0; i < 64; i++ {
		got := computeBackoff(p, 3)
		if got > 3*time.Second {
			t.Fatalf("got %s, expected ≤3s after clamp", got)
		}
	}
}

func pow2(n int) float64 {
	v := 1.0
	for i := 0; i < n; i++ {
		v *= 2
	}
	return v
}

// TestRetry_FailureSchedulesRetry — a handler that fails once, then
// succeeds. With MaxAttempts=3 the dispatcher should pull next_fire
// back to ~now+InitialBackoff and re-run the same fire on a later
// tick, eventually recording a successful run.
func TestRetry_FailureSchedulesRetry(t *testing.T) {
	mem := NewMemoryStorage()
	s, err := New(nil, Options{
		NodeID:        "retry-node",
		TickInterval:  100 * time.Millisecond,
		ShutdownGrace: 300 * time.Millisecond,
	}, WithStorage(mem))
	if err != nil {
		t.Fatal(err)
	}

	var attempts int32
	every, _ := Every(time.Second)
	_, err = s.Schedule("flaky", every, func(_ context.Context) error {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			return errors.New("first attempt fails")
		}
		return nil
	}, WithRetry(RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     400 * time.Millisecond,
		Multiplier:     2.0,
	}))
	if err != nil {
		t.Fatal(err)
	}

	// Seed a stale fire so the dispatcher picks it up on the very
	// first tick instead of waiting a second.
	job := s.Jobs()[0]
	if _, err := mem.EnsureJob(context.Background(), job, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)

	got := atomic.LoadInt32(&attempts)
	if got < 2 {
		t.Fatalf("expected at least 2 attempts (fail + retry succeed); got %d", got)
	}

	runs := mem.Runs()
	if len(runs) < 2 {
		t.Fatalf("expected ≥2 run records; got %d", len(runs))
	}
	if runs[0].Outcome != RunFailed {
		t.Errorf("first run outcome = %q, want %q", runs[0].Outcome, RunFailed)
	}
	if runs[len(runs)-1].Outcome != RunSucceeded {
		t.Errorf("last run outcome = %q, want %q", runs[len(runs)-1].Outcome, RunSucceeded)
	}
	if runs[1].Attempt < 2 {
		t.Errorf("second run attempt = %d, want ≥2", runs[1].Attempt)
	}
}

// TestRetry_ExhaustionMarksDeadLetter — a handler that always fails
// with MaxAttempts=2 should produce two RunFailed-or-DeadLetter records
// and then move on (next_fire advances past the failure window).
func TestRetry_ExhaustionMarksDeadLetter(t *testing.T) {
	mem := NewMemoryStorage()
	s, err := New(nil, Options{
		NodeID:        "dlq-node",
		TickInterval:  100 * time.Millisecond,
		ShutdownGrace: 300 * time.Millisecond,
	}, WithStorage(mem))
	if err != nil {
		t.Fatal(err)
	}

	var attempts int32
	every, _ := Every(time.Second)
	_, err = s.Schedule("doomed", every, func(_ context.Context) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("always fails")
	}, WithRetry(RetryPolicy{
		MaxAttempts:    2,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2.0,
	}))
	if err != nil {
		t.Fatal(err)
	}

	job := s.Jobs()[0]
	if _, err := mem.EnsureJob(context.Background(), job, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)

	got := atomic.LoadInt32(&attempts)
	if got < 2 {
		t.Fatalf("expected at least 2 attempts; got %d", got)
	}

	runs := mem.Runs()
	if len(runs) == 0 {
		t.Fatal("no runs recorded")
	}
	// Last recorded run for the exhausted fire should be DeadLetter.
	var sawDeadLetter bool
	for _, r := range runs {
		if r.Outcome == RunDeadLetter {
			sawDeadLetter = true
			break
		}
	}
	if !sawDeadLetter {
		t.Errorf("expected at least one RunDeadLetter outcome; got %+v",
			outcomes(runs))
	}
}

func outcomes(runs []RunRecord) []RunOutcome {
	out := make([]RunOutcome, 0, len(runs))
	for _, r := range runs {
		out = append(out, r.Outcome)
	}
	return out
}
