// Package telemetry configures OpenTelemetry tracing and carries W3C trace
// context inside job rows, so one trace spans the enqueue request and the
// job's execution on whichever worker eventually claims it.
package telemetry

import (
	"context"
	"encoding/json"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var Tracer = otel.Tracer("github.com/pavan-dangeti/Conveyor")

// Setup installs the W3C propagator and a sampling tracer provider. Spans
// are exported over OTLP/HTTP only when OTEL_EXPORTER_OTLP_ENDPOINT is set;
// without it trace IDs are still generated and propagated into jobs.
func Setup(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Inject serialises the trace context of ctx for storage in
// jobs.trace_context, e.g. {"traceparent":"00-<trace>-<span>-01"}.
// It returns nil when ctx carries no span.
func Inject(ctx context.Context) json.RawMessage {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	b, _ := json.Marshal(carrier)
	return b
}

// Extract returns ctx with the stored trace context as the remote parent.
func Extract(ctx context.Context, stored json.RawMessage) context.Context {
	carrier := propagation.MapCarrier{}
	if len(stored) == 0 || json.Unmarshal(stored, &carrier) != nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
