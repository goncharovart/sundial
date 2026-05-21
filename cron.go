package sundial

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// cronParser is the package-wide cron parser. It is created once at init time;
// parsers in robfig/cron are safe for concurrent use.
//
// Sundial accepts standard 5-field expressions (minute, hour, day of month,
// month, day of week) and the documented robfig descriptors ("@hourly",
// "@daily", "@weekly", "@monthly", "@yearly"). Seconds are intentionally not
// supported — production cron rarely needs sub-minute precision and tightening
// the surface keeps the parser predictable.
var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

type cronSchedule struct {
	expr   string
	parsed cron.Schedule
}

// Cron parses a 5-field cron expression and returns a Schedule.
// Returns ErrInvalidSchedule wrapped with the parser error on bad input.
func Cron(expr string) (Schedule, error) {
	parsed, err := cronParser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("%w: %q (%v)", ErrInvalidSchedule, expr, err)
	}
	return cronSchedule{expr: expr, parsed: parsed}, nil
}

func (s cronSchedule) Next(after time.Time) time.Time {
	return s.parsed.Next(after)
}

func (s cronSchedule) String() string { return s.expr }
func (s cronSchedule) Kind() string   { return "cron" }
