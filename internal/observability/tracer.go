package observability

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const TracerName = "financial-ledger"

// Tracer, global OpenTelemetry tracer örneğini döner.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// InitTracer, OpenTelemetry dağıtık izleme (distributed tracing) altyapısını başlatır.
// Eğer writer nil verilirse io.Discard kullanılır (testler ve sessiz mod için).
func InitTracer(ctx context.Context, serviceName string, writer io.Writer) (*sdktrace.TracerProvider, func(context.Context) error, error) {
	if writer == nil {
		writer = io.Discard
	}

	exporter, err := stdouttrace.New(
		stdouttrace.WithWriter(writer),
		stdouttrace.WithoutTimestamps(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("trace exporter oluşturulamadı: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("1.0.0"),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("trace resource oluşturulamadı: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	shutdown := func(shutdownCtx context.Context) error {
		return tp.Shutdown(shutdownCtx)
	}

	return tp, shutdown, nil
}
