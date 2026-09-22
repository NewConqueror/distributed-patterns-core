package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

const (
	TracerName = "task-orchestrator"
)

// TracingManager manages distributed context propagation and OpenTelemetry spans.
type TracingManager struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// NewTracingManager initializes OpenTelemetry tracer and W3C TraceContext propagator.
func NewTracingManager(tp trace.TracerProvider) *TracingManager {
	if tp == nil {
		tp = sdktrace.NewTracerProvider()
	}

	otel.SetTracerProvider(tp)
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	otel.SetTextMapPropagator(propagator)

	return &TracingManager{
		tracer:     tp.Tracer(TracerName),
		propagator: propagator,
	}
}

// InjectTraceContext injects the current trace context (TraceID & SpanID) from ctx into job.Metadata.
// Called on the Producer side before enqueuing the task.
func (tm *TracingManager) InjectTraceContext(ctx context.Context, job *task.Job) {
	if tm == nil || job == nil {
		return
	}
	tm.propagator.Inject(ctx, job.Carrier())
}

// StartJobSpan extracts trace context from job.Metadata and starts a new span representing the worker execution.
// Called on the Worker side when a job begins processing.
func (tm *TracingManager) StartJobSpan(ctx context.Context, job *task.Job, workerID string) (context.Context, trace.Span) {
	if tm == nil || job == nil {
		return ctx, trace.SpanFromContext(ctx)
	}

	// Extract parent trace context propagated from producer
	extractedCtx := tm.propagator.Extract(ctx, job.Carrier())

	spanName := fmt.Sprintf("job:%s", job.Type)
	newCtx, span := tm.tracer.Start(extractedCtx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("job.id", job.ID),
			attribute.String("job.type", job.Type),
			attribute.Int("job.retry_count", job.RetryCount),
			attribute.Int("job.max_retries", job.MaxRetries),
			attribute.String("worker.id", workerID),
		),
	)

	return newCtx, span
}

// RecordSpanError records the error, sets error status, and logs error details onto the span.
func (tm *TracingManager) RecordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// RecordSpanSuccess marks the span as cleanly completed.
func (tm *TracingManager) RecordSpanSuccess(span trace.Span) {
	if span == nil {
		return
	}
	span.SetStatus(codes.Ok, "completed")
}
