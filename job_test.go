package sundial

import (
	"testing"
	"time"
)

func TestJobOptions_withDefaults(t *testing.T) {
	t.Run("zero retry policy gets defaults", func(t *testing.T) {
		got := JobOptions{}.withDefaults()
		if got.Retry.MaxAttempts != 3 {
			t.Errorf("MaxAttempts = %d, want 3", got.Retry.MaxAttempts)
		}
		if got.Retry.InitialBackoff != time.Second {
			t.Errorf("InitialBackoff = %s, want 1s", got.Retry.InitialBackoff)
		}
		if got.Retry.MaxBackoff != 5*time.Minute {
			t.Errorf("MaxBackoff = %s, want 5m", got.Retry.MaxBackoff)
		}
		if got.Retry.Multiplier != 2.0 {
			t.Errorf("Multiplier = %v, want 2.0", got.Retry.Multiplier)
		}
	})

	t.Run("explicit retry policy is preserved", func(t *testing.T) {
		in := JobOptions{
			Retry: RetryPolicy{
				MaxAttempts:    7,
				InitialBackoff: 250 * time.Millisecond,
				MaxBackoff:     30 * time.Second,
				Multiplier:     1.5,
			},
		}
		got := in.withDefaults()
		if got.Retry != in.Retry {
			t.Errorf("retry policy mutated: got %+v, want %+v", got.Retry, in.Retry)
		}
	})

	t.Run("partial retry policy fills only zero fields", func(t *testing.T) {
		in := JobOptions{Retry: RetryPolicy{MaxAttempts: 10}}
		got := in.withDefaults()
		if got.Retry.MaxAttempts != 10 {
			t.Errorf("explicit MaxAttempts overwritten: got %d", got.Retry.MaxAttempts)
		}
		if got.Retry.InitialBackoff != time.Second {
			t.Errorf("InitialBackoff not defaulted: got %s", got.Retry.InitialBackoff)
		}
	})

	t.Run("LeaderOnly and MissedFire pass through", func(t *testing.T) {
		in := JobOptions{LeaderOnly: true, MissedFire: MissedFireSkip}
		got := in.withDefaults()
		if !got.LeaderOnly {
			t.Error("LeaderOnly flipped to false")
		}
		if got.MissedFire != MissedFireSkip {
			t.Errorf("MissedFire = %v, want MissedFireSkip", got.MissedFire)
		}
	})
}
