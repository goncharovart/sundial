package sundial

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestJob(t *testing.T, name string) *Job {
	t.Helper()
	sched, err := Every(time.Minute)
	if err != nil {
		t.Fatalf("Every failed: %v", err)
	}
	return &Job{
		Name:     name,
		Schedule: sched,
		Handler:  func(context.Context) error { return nil },
		Options:  JobOptions{}.withDefaults(),
	}
}

func TestMemoryStorage_EnsureJob_InsertAndUpdate(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

	job := newTestJob(t, "nightly")
	id1, err := s.EnsureJob(ctx, job, now)
	if err != nil {
		t.Fatal(err)
	}
	if id1 == "" {
		t.Fatal("EnsureJob returned empty id")
	}

	// Re-ensure with the same name should return the same id.
	id2, err := s.EnsureJob(ctx, job, now)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("re-ensure id mismatch: %q vs %q", id1, id2)
	}
}

func TestMemoryStorage_EnsureJob_RejectsNil(t *testing.T) {
	s := NewMemoryStorage()
	if _, err := s.EnsureJob(context.Background(), nil, time.Now()); err == nil {
		t.Fatal("expected error for nil job")
	}
}

func TestMemoryStorage_FetchDue(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

	// Job A is due at base; job B is due at base+5m.
	_, _ = s.EnsureJob(ctx, newTestJob(t, "a"), base)
	_, _ = s.EnsureJob(ctx, newTestJob(t, "b"), base.Add(5*time.Minute))

	due, err := s.FetchDue(ctx, base.Add(time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Name != "a" {
		t.Fatalf("unexpected due at base+1m: %+v", due)
	}

	due, _ = s.FetchDue(ctx, base.Add(10*time.Minute), 10)
	if len(due) != 2 {
		t.Fatalf("expected both due at base+10m, got %d", len(due))
	}
	if due[0].Name != "a" || due[1].Name != "b" {
		t.Errorf("expected order a,b; got %s,%s", due[0].Name, due[1].Name)
	}
}

func TestMemoryStorage_FetchDue_Limit(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		_, _ = s.EnsureJob(ctx, newTestJob(t, n), base)
	}
	due, _ := s.FetchDue(ctx, base.Add(time.Minute), 3)
	if len(due) != 3 {
		t.Fatalf("limit=3, got %d", len(due))
	}
}

func TestMemoryStorage_ClaimJob(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

	id, _ := s.EnsureJob(ctx, newTestJob(t, "job"), base)

	t.Run("first claim wins", func(t *testing.T) {
		ok, err := s.ClaimJob(ctx, id, base.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected first claim to succeed")
		}
	})

	t.Run("second claim with same fire is rejected", func(t *testing.T) {
		// next_fire is now base+1m; trying to claim again with base+1m
		// should fail because next_fire is NOT before the requested.
		ok, err := s.ClaimJob(ctx, id, base.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected second claim to fail (next_fire already advanced)")
		}
	})

	t.Run("claim of unknown job returns ErrJobNotFound", func(t *testing.T) {
		_, err := s.ClaimJob(ctx, "no-such-id", base.Add(time.Hour))
		if !errors.Is(err, ErrJobNotFound) {
			t.Fatalf("expected ErrJobNotFound, got %v", err)
		}
	})
}

func TestMemoryStorage_RecordRun(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		err := s.RecordRun(ctx, RunRecord{
			JobID:   "j1",
			Outcome: RunSucceeded,
			NodeID:  "node-1",
		})
		if err != nil {
			t.Fatalf("RecordRun iteration %d: %v", i, err)
		}
	}

	runs := s.Runs()
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
	for _, r := range runs {
		if r.JobID != "j1" {
			t.Errorf("unexpected JobID: %q", r.JobID)
		}
		if r.Attempt < 1 {
			t.Errorf("attempt not defaulted: %d", r.Attempt)
		}
		if r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
			t.Error("StartedAt / FinishedAt not defaulted")
		}
	}
}

func TestAdvisoryKey_Deterministic(t *testing.T) {
	a := advisoryKey("00000000-0000-0000-0000-000000000001")
	b := advisoryKey("00000000-0000-0000-0000-000000000001")
	if a != b {
		t.Fatalf("advisoryKey not deterministic: %d vs %d", a, b)
	}
}

func TestAdvisoryKey_DifferentIDs(t *testing.T) {
	a := advisoryKey("00000000-0000-0000-0000-000000000001")
	b := advisoryKey("00000000-0000-0000-0000-000000000002")
	if a == b {
		t.Fatalf("distinct ids hash to same key: %d", a)
	}
}
