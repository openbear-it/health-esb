package events

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
)

const envelopeVersion = "1.0"

// MessageEnvelope wraps every message payload with routing and tracing metadata.
// It is the canonical wire format for all messages on the health-esb bus.
type MessageEnvelope struct {
	// ID is a unique UUID v4 generated at publish time.
	ID string `json:"id"`
	// CorrelationID is propagated unchanged across the whole processing chain.
	CorrelationID string `json:"correlation_id"`
	// CausationID holds the ID of the message that caused this one.
	CausationID string `json:"causation_id"`
	// Version allows receivers to handle schema evolution gracefully.
	Version string `json:"version"`
	// Source is the originating service name (SERVICE_NAME env var by default).
	Source string `json:"source"`
	// Type identifies the event schema, e.g. "hl7.adt.a01", "fhir.patient.created".
	Type string `json:"type"`
	// Timestamp is the UTC moment the envelope was created.
	Timestamp time.Time `json:"timestamp"`
	// Headers carries optional key-value metadata (tenant, trace-id, etc.).
	Headers map[string]string `json:"headers,omitempty"`
	// Payload is the opaque JSON-encoded domain payload.
	Payload json.RawMessage `json:"payload"`
}

// MarshalEnvelope builds a MessageEnvelope around payload and returns its
// JSON encoding. A UUID v4 ID and current UTC timestamp are generated
// automatically.
//
// source defaults to the SERVICE_NAME environment variable when empty.
// headers may be nil.
func MarshalEnvelope(
	msgType, correlationID, causationID, source string,
	headers map[string]string,
	payload any,
) ([]byte, error) {
	if source == "" {
		source = os.Getenv("SERVICE_NAME")
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope payload: %w", err)
	}

	env := MessageEnvelope{
		ID:            uuid.New().String(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		Version:       envelopeVersion,
		Source:        source,
		Type:          msgType,
		Timestamp:     time.Now().UTC(),
		Headers:       headers,
		Payload:       raw,
	}

	b, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	return b, nil
}

// UnmarshalEnvelope decodes JSON bytes into a MessageEnvelope.
func UnmarshalEnvelope(data []byte) (*MessageEnvelope, error) {
	var env MessageEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	return &env, nil
}
