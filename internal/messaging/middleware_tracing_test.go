package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newTestTP creates an in-memory TracerProvider for testing.
func newTestTP() (*trace.TracerProvider, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(rec))
	return tp, rec
}

func buildEnvelopeMsg(t *testing.T, msgType string, headers map[string]string) *message.Message {
	t.Helper()
	b, err := events.MarshalEnvelope(msgType, "corr", "cause", "svc", headers, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	msg := message.NewMessage(watermill.NewUUID(), b)
	msg.SetContext(context.Background())
	return msg
}

func TestTracingMiddleware_SpanStartedForEnvelope(t *testing.T) {
	tp, rec := newTestTP()
	mw := messaging.TracingMiddleware("test-service", tp)
	handler := applyMiddleware(mw, func(msg *message.Message) ([]*message.Message, error) {
		return nil, nil
	})

	msg := buildEnvelopeMsg(t, "test.event", nil)
	if _, err := handler(msg); err != nil {
		t.Fatal(err)
	}

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "test-service/test.event/handle" {
		t.Errorf("span name = %q", spans[0].Name())
	}
}

func TestTracingMiddleware_HandlerErrorSetsErrorStatus(t *testing.T) {
	tp, rec := newTestTP()
	mw := messaging.TracingMiddleware("svc", tp)
	handler := applyMiddleware(mw, func(msg *message.Message) ([]*message.Message, error) {
		return nil, errors.New("boom")
	})

	msg := buildEnvelopeMsg(t, "bad.event", nil)
	if _, err := handler(msg); err == nil {
		t.Fatal("expected error from handler")
	}

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Status().Code != codes.Error {
		t.Errorf("expected ERROR status, got %v", spans[0].Status())
	}
}

func TestTracingMiddleware_NonEnvelopePassesThrough(t *testing.T) {
	tp, rec := newTestTP()
	mw := messaging.TracingMiddleware("svc", tp)
	called := false
	handler := applyMiddleware(mw, func(msg *message.Message) ([]*message.Message, error) {
		called = true
		return nil, nil
	})

	msg := message.NewMessage(watermill.NewUUID(), []byte(`{"not":"envelope"}`))
	msg.SetContext(context.Background())
	if _, err := handler(msg); err != nil {
		t.Fatal(err)
	}

	if !called {
		t.Error("handler not called for non-envelope message")
	}
	if len(rec.Ended()) != 0 {
		t.Error("no span should be created for non-envelope message")
	}
}

func TestTracingMiddleware_TraceparentPropagated(t *testing.T) {
	// Start a span and inject its traceparent into the envelope header,
	// then verify the middleware creates a child span.
	tp, rec := newTestTP()

	// Create a parent span to get a real trace ID.
	tracer := tp.Tracer("test")
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	parentSpan.End()

	traceID := parentSpan.SpanContext().TraceID().String()
	spanID := parentSpan.SpanContext().SpanID().String()
	traceparent := "00-" + traceID + "-" + spanID + "-01"

	mw := messaging.TracingMiddleware("svc", tp)
	handler := applyMiddleware(mw, func(msg *message.Message) ([]*message.Message, error) {
		return nil, nil
	})

	msg := buildEnvelopeMsg(t, "tp.event", map[string]string{"traceparent": traceparent})
	msg.SetContext(parentCtx)

	if _, err := handler(msg); err != nil {
		t.Fatal(err)
	}

	spans := rec.Ended()
	// parentSpan + child span
	if len(spans) < 2 {
		t.Fatalf("expected >= 2 spans, got %d", len(spans))
	}
	child := spans[len(spans)-1]
	if child.Parent().TraceID() != parentSpan.SpanContext().TraceID() {
		t.Errorf("child span not linked to parent trace")
	}
}

func TestInjectSpanIntoEnvelope_SetsTraceparent(t *testing.T) {
	tp, _ := newTestTP()
	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	env := &events.MessageEnvelope{
		ID:      "id1",
		Version: "1.0",
		Type:    "t",
		Payload: json.RawMessage(`{}`),
	}
	messaging.InjectSpanIntoEnvelope(ctx, env)

	if env.Headers["traceparent"] == "" {
		t.Error("traceparent header must be set after inject")
	}
}

func TestInjectSpanIntoEnvelope_NilHeadersInitialised(t *testing.T) {
	tp, _ := newTestTP()
	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "s")
	defer span.End()

	env := &events.MessageEnvelope{Version: "1.0", Payload: json.RawMessage(`{}`)}
	if env.Headers != nil {
		t.Fatal("precondition: headers must be nil")
	}
	messaging.InjectSpanIntoEnvelope(ctx, env)
	// Even if no span is active, headers map must be initialised.
	if env.Headers == nil {
		t.Error("Headers must not be nil after InjectSpanIntoEnvelope")
	}
}
