package sundial

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestIterateSchedule_EveryEnumeration(t *testing.T) {
	every, _ := Every(time.Second)
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

	got := iterateSchedule(every, base, base.Add(5*time.Second), 10)
	if len(got) != 4 {
		// Next(base) = base+1s, base+2s, base+3s, base+4s ; base+5s is excluded by strict `<`.
		t.Fatalf("expected 4 instants in (base, base+5s); got %d", len(got))
	}
	for i, ts := range got {
		want := base.Add(time.Duration(i+1) * time.Second)
		if !ts.Equal(want) {
			t.Errorf("instant[%d] = %s, want %s", i, ts, want)
		}
	}
}

func TestIterateSchedule_RespectsLimit(t *testing.T) {
	every, _ := Every(time.Second)
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	got := iterateSchedule(every, base, base.Add(time.Hour), 3)
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 instants under limit=3; got %d", len(got))
	}
}

func TestIterateSchedule_NilAndDegenerate(t *testing.T) {
	if got := iterateSchedule(nil, time.Now(), time.Now().Add(time.Hour), 10); got != nil {
		t.Errorf("nil schedule: got %v, want nil", got)
	}
	every, _ := Every(time.Second)
	now := time.Now()
	if got := iterateSchedule(every, now, now, 10); got != nil {
		t.Errorf("empty window: got %v, want nil", got)
	}
	if got := iterateSchedule(every, now.Add(time.Hour), now, 10); got != nil {
		t.Errorf("from > until: got %v, want nil", got)
	}
	if got := iterateSchedule(every, now, now.Add(time.Hour), 0); got != nil {
		t.Errorf("limit=0: got %v, want nil", got)
	}
}

func TestIterateSchedule_AtPastInstantYieldsNothing(t *testing.T) {
	past := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := At(past)
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	got := iterateSchedule(at, now.Add(-time.Hour), now, 10)
	if len(got) != 0 {
		t.Errorf("At() with past instant should yield nothing in (now-1h, now); got %v", got)
	}
}

// TestMissedFireRunAll_ReplaysEveryInstant — when a job's last fire is
// 5 seconds in the past and the policy is RunAll, the handler must
// execute once per missed second. With a 1s cadence and a 5s window,
// we expect roughly 4 catch-up runs (strict `<` boundary excludes the
// "now" instant the regular loop would have picked up anyway).
func TestMissedFireRunAll_ReplaysEveryInstant(t *testing.T) {
	mem := NewMemoryStorage()
	s, err := New(nil, Options{
		NodeID:        "runall-node",
		TickInterval:  100 * time.Millisecond,
		ShutdownGrace: 300 * time.Millisecond,
	}, WithStorage(mem))
	if err != nil {
		t.Fatal(err)
	}

	var hits int32
	every, _ := Every(time.Second)
	job, err := s.Schedule("audit", every,
		func(_ context.Context) error {
			atomic.AddInt32(&hits, 1)
			return nil
		},
		WithMissedFire(MissedFireRunAll),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Seed next_fire 5 seconds in the past so the iterator has a
	// real window to replay.
	if _, err := mem.EnsureJob(context.Background(), job, time.Now().Add(-5*time.Second)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)

	got := atomic.LoadInt32(&hits)
	// 4 strict-less-than catch-up + at most 1 regular cadence fire
	// that may or may not slip in inside the 700ms test window.
	if got < 4 || got > 6 {
		t.Fatalf("RunAll hit count = %d, expected 4..6", got)
	}

	runs := mem.Runs()
	if len(runs) < 4 {
		t.Fatalf("expected ≥4 run records (one per replayed instant); got %d", len(runs))
	}
	for i, r := range runs {
		if r.Outcome != RunSucceeded {
			t.Errorf("run[%d] outcome = %q, want %q", i, r.Outcome, RunSucceeded)
		}
	}
}
