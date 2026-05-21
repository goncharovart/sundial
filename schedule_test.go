package sundial

import (
	"errors"
	"testing"
	"time"
)

func TestEvery(t *testing.T) {
	t.Run("rejects sub-second interval", func(t *testing.T) {
		_, err := Every(500 * time.Millisecond)
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("expected ErrInvalidSchedule, got %v", err)
		}
	})

	t.Run("rejects zero interval", func(t *testing.T) {
		_, err := Every(0)
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("expected ErrInvalidSchedule, got %v", err)
		}
	})

	t.Run("Next adds interval", func(t *testing.T) {
		s, err := Every(5 * time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
		got := s.Next(base)
		want := base.Add(5 * time.Minute)
		if !got.Equal(want) {
			t.Fatalf("Next(base) = %s, want %s", got, want)
		}
	})

	t.Run("Kind is every", func(t *testing.T) {
		s, _ := Every(time.Minute)
		if s.Kind() != "every" {
			t.Fatalf("Kind() = %q, want %q", s.Kind(), "every")
		}
	})

	t.Run("String is the duration", func(t *testing.T) {
		s, _ := Every(90 * time.Second)
		if s.String() != "1m30s" {
			t.Fatalf("String() = %q, want %q", s.String(), "1m30s")
		}
	})
}

func TestAt(t *testing.T) {
	target := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	s := At(target)

	t.Run("Next returns target when in the future", func(t *testing.T) {
		got := s.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		if !got.Equal(target) {
			t.Fatalf("Next(past) = %s, want %s", got, target)
		}
	})

	t.Run("Next returns zero when target is in the past", func(t *testing.T) {
		got := s.Next(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
		if !got.IsZero() {
			t.Fatalf("Next(future) = %s, want zero time", got)
		}
	})

	t.Run("Kind is at", func(t *testing.T) {
		if s.Kind() != "at" {
			t.Fatalf("Kind() = %q, want %q", s.Kind(), "at")
		}
	})
}

func TestCron_NotYetImplemented(t *testing.T) {
	_, err := Cron("0 3 * * *")
	if !errors.Is(err, ErrInvalidSchedule) {
		t.Fatalf("expected ErrInvalidSchedule wrapper while cron is not implemented, got %v", err)
	}
}
