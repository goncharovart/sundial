package sundial

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

// telemetry packages the OpenTelemetry instruments the dispatcher emits.
// All instruments are derived from the global meter / tracer providers
// so callers integrate Sundial with whatever exporter pipeline they
// already wired up (stdout, OTLP, Tempo, Jaeger).
//
// If the global meter/tracer providers were never configured, all the
// metric instruments fall back to a no-op meter so the dispatcher can
// still operate; the tracer also falls back to a noop tracer via the
// otel package defaults.
type telemetry struct {
	tracer trace.Tracer

	jobsScheduled   metric.Int64UpDownCounter
	jobsRunning     metric.Int64UpDownCounter
	jobDuration     metric.Float64Histogram
	jobLag          metric.Float64Histogram
	jobFailures     metric.Int64Counter
	jobClaimedTotal metric.Int64Counter
}

// instrumentationName must match the package import path so vendor
// dashboards can pivot by instrumentation.
const instrumentationName = "github.com/goncharovart/sundial"

// newTelemetry builds the dispatcher's instruments. It never returns an
// error — instrumentation failures degrade to no-ops, never to startup
// failures.
func newTelemetry() *telemetry {
	t := &telemetry{
		tracer: otel.GetTracerProvider().Tracer(instrumentationName),
	}

	meter := otel.GetMeterProvider().Meter(instrumentationName)
	if meter == nil {
		meter = noop.NewMeterProvider().Meter(instrumentationName)
	}

	// Best-effort instrument construction: any failure -> noop fallback.
	noopMeter := noop.NewMeterProvider().Meter(instrumentationName)

	var err error
	if t.jobsScheduled, err = meter.Int64UpDownCounter(
		"sundial.jobs.scheduled",
		metric.WithDescription("Currently registered jobs."),
	); err != nil {
		t.jobsScheduled, _ = noopMeter.Int64UpDownCounter("sundial.jobs.scheduled")
	}
	if t.jobsRunning, err = meter.Int64UpDownCounter(
		"sundial.jobs.running",
		metric.WithDescription("Jobs currently executing in this process."),
	); err != nil {
		t.jobsRunning, _ = noopMeter.Int64UpDownCounter("sundial.jobs.running")
	}
	if t.jobDuration, err = meter.Float64Histogram(
		"sundial.jobs.duration_seconds",
		metric.WithDescription("Handler execution time."),
		metric.WithUnit("s"),
	); err != nil {
		t.jobDuration, _ = noopMeter.Float64Histogram("sundial.jobs.duration_seconds")
	}
	if t.jobLag, err = meter.Float64Histogram(
		"sundial.jobs.lag_seconds",
		metric.WithDescription("Observed delay between the scheduled fire and the actual start."),
		metric.WithUnit("s"),
	); err != nil {
		t.jobLag, _ = noopMeter.Float64Histogram("sundial.jobs.lag_seconds")
	}
	if t.jobFailures, err = meter.Int64Counter(
		"sundial.jobs.failures_total",
		metric.WithDescription("Job executions ending in failure."),
	); err != nil {
		t.jobFailures, _ = noopMeter.Int64Counter("sundial.jobs.failures_total")
	}
	if t.jobClaimedTotal, err = meter.Int64Counter(
		"sundial.jobs.claimed_total",
		metric.WithDescription("Successful claim transactions, by outcome."),
	); err != nil {
		t.jobClaimedTotal, _ = noopMeter.Int64Counter("sundial.jobs.claimed_total")
	}
	return t
}

// startRunSpan opens the span that wraps a single execution attempt.
// The returned context is what the handler receives, so any nested
// spans the handler creates will be children of this one.
func (t *telemetry) startRunSpan(
	ctx context.Context, jobName, jobID, nodeID, scheduleKind string, fireTime time.Time,
) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, "sundial.job.run",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("sundial.job.name", jobName),
			attribute.String("sundial.job.id", jobID),
			attribute.String("sundial.job.schedule_kind", scheduleKind),
			attribute.String("sundial.node.id", nodeID),
			attribute.String("sundial.fire_time", fireTime.UTC().Format(time.RFC3339Nano)),
		),
	)
}

// recordLag emits the gap between scheduled fire and actual start.
func (t *telemetry) recordLag(ctx context.Context, jobName string, fire, started time.Time) {
	t.jobLag.Record(ctx, started.Sub(fire).Seconds(),
		metric.WithAttributes(attribute.String("sundial.job.name", jobName)),
	)
}

// recordDuration emits handler execution time and ticks failure / claim
// counters where appropriate.
func (t *telemetry) recordDuration(
	ctx context.Context, span trace.Span, jobName string, dur time.Duration, outcome RunOutcome,
) {
	attrs := metric.WithAttributes(
		attribute.String("sundial.job.name", jobName),
		attribute.String("sundial.outcome", string(outcome)),
	)
	t.jobDuration.Record(ctx, dur.Seconds(), attrs)
	if outcome != RunSucceeded {
		t.jobFailures.Add(ctx, 1, attrs)
		span.SetStatus(codes.Error, "job did not succeed")
	} else {
		span.SetStatus(codes.Ok, "")
	}
}

// claimedInc records a successful claim transaction (one claim per
// (node, job) per fire).
func (t *telemetry) claimedInc(ctx context.Context, jobName string) {
	t.jobClaimedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("sundial.job.name", jobName),
	))
}
