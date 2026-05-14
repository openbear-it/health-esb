package messaging

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/events"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// mapCarrier adapts map[string]string to propagation.TextMapCarrier.
type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string {
	if m == nil {
		return ""
	}
	return m[key]
}
func (m mapCarrier) Set(key, value string) {
	if m != nil {
		m[key] = value
	}
}
func (m mapCarrier) Keys() []string {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = mapCarrier(nil)

// TracingMiddleware returns a Watermill HandlerMiddleware that propagates
// OpenTelemetry trace context through MessageEnvelope.Headers (W3C format).
//
// On consume: reads "traceparent" from the envelope Headers, reconstructs the
// parent SpanContext, starts a child span named "<serviceName>/<topic>/handle",
// and ends it with OK or ERROR status depending on the handler result.
func TracingMiddleware(serviceName string, tp trace.TracerProvider) message.HandlerMiddleware {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	tracer := tp.Tracer("health-esb/messaging")
	// Use W3C TraceContext propagator directly for deterministic behaviour.
	propagator := propagation.TraceContext{}

	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			env, err := events.UnmarshalEnvelope(msg.Payload)
			// Treat as non-envelope if unmarshal fails or Version is empty
			// (empty Version means the JSON was not a real MessageEnvelope).
			if err != nil || env.Version == "" {
				// Not an envelope — pass through without tracing.
				return next(msg)
			}

			// Reconstruct parent context from W3C traceparent in envelope headers.
			carrier := mapCarrier(env.Headers)
			parentCtx := propagator.Extract(msg.Context(), carrier)

			topic := env.Type
			if topic == "" {
				topic = "unknown"
			}
			spanName := serviceName + "/" + topic + "/handle"

			ctx, span := tracer.Start(parentCtx, spanName,
				trace.WithSpanKind(trace.SpanKindConsumer),
			)

			msg.SetContext(ctx)
			out, handlerErr := next(msg)

			if handlerErr != nil {
				span.SetStatus(codes.Error, handlerErr.Error())
				span.RecordError(handlerErr)
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()

			return out, handlerErr
		}
	}
}

// InjectSpanIntoEnvelope writes the W3C traceparent of the active span in ctx
// into env.Headers. Call this before marshalling the envelope for publishing.
func InjectSpanIntoEnvelope(ctx context.Context, env *events.MessageEnvelope) {
	if env.Headers == nil {
		env.Headers = make(map[string]string)
	}
	// Use W3C TraceContext propagator directly — no dependency on global state.
	propagation.TraceContext{}.Inject(ctx, mapCarrier(env.Headers))
}
