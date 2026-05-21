package sundial

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// seedPastJob registers `name` and seeds it directly in storage with a
// next_fire pointing into the past, so the very first dispatcher tick
// classifies it as "missed". Returns the job that was registered with
// the scheduler so the caller can read its handler.
func seedPastJob(
	t *testing.T,
	s *Scheduler,
	mem *MemoryStorage,
	name string,
	pastBy time.Duration,
	policy MissedFirePolicy,
	handler HandlerFunc,
) {
	t.Helper()
	every, err := Every(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Schedule(name, every, handler, WithMissedFire(policy))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mem.EnsureJob(context.Background(), job, time.Now().Add(-pastBy)); err != nil {
		t.Fatal(err)
	}
}

// TestMissedFireSkip — when next_fire is far in the past and the
// policy is MissedFireSkip, the handler must NOT run; only the
// next_fire pointer should advance into the future.
func TestMissedFireSkip(t *testing.T) {
	mem := NewMemoryStorage()
	s, err := New(nil, Options{
		NodeID:        "skip-node",
		TickInterval:  100 * time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	}, WithStorage(mem))
	if err != nil {
		t.Fatal(err)
	}

	var hits int32
	seedPastJob(t, s, mem, "late-bird", 2*time.Second, MissedFireSkip,
		func(_ context.Context) error {
			atomic.AddInt32(&hits, 1)
			return nil
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)

	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Fatalf("MissedFireSkip ran the handler %d times; expected 0", got)
	}
	if len(mem.Runs()) != 0 {
		t.Errorf("MissedFireSkip recorded a run: %+v", mem.Runs())
	}
	for _, j := range mem.jobs {
		if !j.NextFireTime.After(time.Now()) {
			t.Errorf("next_fire still in the past after Skip: %s", j.NextFireTime)
		}
	}
}

// TestMissedFireRunOnce — when next_fire is far in the past and the
// policy is the default (RunOnce), the handler runs at least once
// (the catch-up) and then next_fire advances forward so we don't
// thrash on the same missed window.
func TestMissedFireRunOnce(t *testing.T) {
	mem := NewMemoryStorage()
	s, err := New(nil, Options{
		NodeID:        "runonce-node",
		TickInterval:  100 * time.Millisecond,
		ShutdownGrace: 300 * time.Millisecond,
	}, WithStorage(mem))
	if err != nil {
		t.Fatal(err)
	}

	var hits int32
	seedPastJob(t, s, mem, "late-bird", 2*time.Second, MissedFireRunOnce,
		func(_ context.Context) error {
			atomic.AddInt32(&hits, 1)
			return nil
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)

	got := atomic.LoadInt32(&hits)
	if got == 0 {
		t.Fatal("RunOnce did not execute the catch-up handler")
	}
	if got > 2 {
		t.Errorf("RunOnce ran %d times; expected ≤2 (catch-up plus at most one cadence)", got)
	}
	for _, j := range mem.jobs {
		if !j.NextFireTime.After(time.Now()) {
			t.Errorf("next_fire still in the past after RunOnce: %s", j.NextFireTime)
		}
	}
}
