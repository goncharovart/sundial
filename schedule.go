// Package sundial provides a durable, distributed cron scheduler backed by Postgres.
//
// See README.md for an overview and docs/design.md for the architectural design.
package sundial

import (
	"errors"
	"fmt"
	"time"
)

// Schedule defines when a job should fire.
//
// Implementations are pure: given a moment in time, Next returns the next fire
// time strictly after it. Schedules must be deterministic — two calls with the
// same input always return the same output. This invariant lets Sundial recover
// missed fires by re-deriving them from the schedule alone.
type Schedule interface {
	// Next returns the first fire time strictly after `after`.
	// It returns the zero Time if no further fire is scheduled.
	Next(after time.Time) time.Time

	// String returns a human-readable representation of the schedule.
	// Sundial uses it both for storage and for logs/UI.
	String() string

	// Kind returns the schedule family ("cron", "every", or "at").
	// It is stored alongside String() to disambiguate parsing on load.
	Kind() string
}

// ErrInvalidSchedule is returned by schedule parsers when the input is not a
// well-formed schedule of the expected kind.
var ErrInvalidSchedule = errors.New("sundial: invalid schedule")

// intervalSchedule fires every `d` after a reference instant.
type intervalSchedule struct {
	d time.Duration
}

// Every returns a Schedule that fires every d after registration.
// Sub-second intervals are rejected to prevent dispatcher overload.
func Every(d time.Duration) (Schedule, error) {
	if d < time.Second {
		return nil, fmt.Errorf("%w: interval must be ≥ 1s, got %s", ErrInvalidSchedule, d)
	}
	return intervalSchedule{d: d}, nil
}

func (s intervalSchedule) Next(after time.Time) time.Time {
	return after.Add(s.d)
}

func (s intervalSchedule) String() string { return s.d.String() }
func (s intervalSchedule) Kind() string   { return "every" }

// atSchedule fires once at a specific instant; subsequent calls return zero.
type atSchedule struct {
	t time.Time
}

// At returns a Schedule that fires exactly once at t.
// Past instants are accepted at registration time; missed-fire policy decides
// whether they execute on the next leader tick.
func At(t time.Time) Schedule {
	return atSchedule{t: t.UTC()}
}

func (s atSchedule) Next(after time.Time) time.Time {
	if s.t.After(after) {
		return s.t
	}
	return time.Time{}
}

func (s atSchedule) String() string { return s.t.Format(time.RFC3339) }
func (s atSchedule) Kind() string   { return "at" }

// Cron is defined in cron.go to keep the robfig/cron import isolated from the
// rest of the package surface.
