package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtractRoundTrip(t *testing.T) {
	shutdown, err := Setup(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = shutdown(context.Background()) }()
	if Inject(context.Background()) != nil {
		t.Fatal("no span should inject nothing")
	}
	ctx, span := Tracer.Start(context.Background(), "enqueue")
	defer span.End()
	stored := Inject(ctx)
	got := trace.SpanContextFromContext(Extract(context.Background(), stored))
	if got.TraceID() != span.SpanContext().TraceID() || !got.IsRemote() {
		t.Fatalf("round trip lost the trace: %s", stored)
	}
	if Extract(context.Background(), []byte("not json")) != context.Background() {
		t.Fatal("garbage must leave the context untouched")
	}
}

func TestSetupWithExporter(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	shutdown, err := Setup(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = shutdown(ctx)
}
