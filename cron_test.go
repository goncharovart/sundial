package sundial

import (
	"errors"
	"testing"
	"time"
)

func TestCron(t *testing.T) {
	t.Run("parses 5-field expression", func(t *testing.T) {
		s, err := Cron("0 3 * * *") // daily at 03:00
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.Kind() != "cron" {
			t.Fatalf("Kind() = %q, want %q", s.Kind(), "cron")
		}
		if s.String() != "0 3 * * *" {
			t.Fatalf("String() = %q, want %q", s.String(), "0 3 * * *")
		}
	})

	t.Run("supports descriptors", func(t *testing.T) {
		s, err := Cron("@hourly")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.Kind() != "cron" {
			t.Fatalf("Kind() = %q, want cron", s.Kind())
		}
	})

	t.Run("Next gives the right fire time", func(t *testing.T) {
		s, _ := Cron("0 3 * * *")
		// 12:00 UTC → next fire is 03:00 next day UTC.
		base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
		got := s.Next(base)
		want := time.Date(2026, 5, 22, 3, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("Next(%s) = %s, want %s", base, got, want)
		}
	})

	t.Run("rejects invalid expression", func(t *testing.T) {
		_, err := Cron("not a cron")
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("expected ErrInvalidSchedule, got %v", err)
		}
	})

	t.Run("rejects 6-field (seconds) expression", func(t *testing.T) {
		// "0 0 3 * * *" would be a 6-field schedule; we deliberately do not
		// enable cron.Second so this must fail.
		_, err := Cron("0 0 3 * * *")
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("expected ErrInvalidSchedule for 6-field input, got %v", err)
		}
	})
}
