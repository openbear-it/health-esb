package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/openbear-it/health-esb/internal/events"
)

func TestMarshalEnvelope(t *testing.T) {
	type payload struct {
		PatientID string `json:"patient_id"`
	}

	tests := []struct {
		name          string
		msgType       string
		correlationID string
		causationID   string
		source        string
		headers       map[string]string
		payload       any
		wantErr       bool
	}{
		{
			name:          "full envelope",
			msgType:       "hl7.adt.a01",
			correlationID: "corr-123",
			causationID:   "cause-456",
			source:        "adt-service",
			headers:       map[string]string{"x-tenant": "hospital-1"},
			payload:       payload{PatientID: "patient-001"},
		},
		{
			name:    "nil headers",
			msgType: "fhir.patient.created",
			source:  "fhir-bridge",
			payload: payload{PatientID: "patient-002"},
		},
		{
			name:    "empty strings are accepted",
			msgType: "test.event",
			payload: payload{},
		},
		{
			name:    "unmarshalable payload",
			msgType: "test",
			payload: make(chan int), // channels cannot be JSON-marshalled
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := events.MarshalEnvelope(tc.msgType, tc.correlationID, tc.causationID, tc.source, tc.headers, tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			env, err := events.UnmarshalEnvelope(b)
			if err != nil {
				t.Fatalf("unmarshal error: %v", err)
			}

			if env.ID == "" {
				t.Error("ID must not be empty")
			}
			if env.Version != "1.0" {
				t.Errorf("Version = %q, want 1.0", env.Version)
			}
			if env.Type != tc.msgType {
				t.Errorf("Type = %q, want %q", env.Type, tc.msgType)
			}
			if env.CorrelationID != tc.correlationID {
				t.Errorf("CorrelationID = %q, want %q", env.CorrelationID, tc.correlationID)
			}
			if env.CausationID != tc.causationID {
				t.Errorf("CausationID = %q, want %q", env.CausationID, tc.causationID)
			}
			if env.Source != tc.source {
				t.Errorf("Source = %q, want %q", env.Source, tc.source)
			}
			if env.Timestamp.IsZero() {
				t.Error("Timestamp must not be zero")
			}
			for k, v := range tc.headers {
				if got := env.Headers[k]; got != v {
					t.Errorf("Headers[%q] = %q, want %q", k, got, v)
				}
			}
		})
	}
}

func TestMarshalEnvelopeUniqueIDs(t *testing.T) {
	b1, _ := events.MarshalEnvelope("test", "", "", "svc", nil, struct{}{})
	b2, _ := events.MarshalEnvelope("test", "", "", "svc", nil, struct{}{})

	env1, _ := events.UnmarshalEnvelope(b1)
	env2, _ := events.UnmarshalEnvelope(b2)

	if env1.ID == env2.ID {
		t.Errorf("IDs must be unique, both are %q", env1.ID)
	}
}

func TestMarshalEnvelopeDefaultSource(t *testing.T) {
	t.Setenv("SERVICE_NAME", "test-service")

	b, err := events.MarshalEnvelope("test.event", "", "", "", nil, struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	env, _ := events.UnmarshalEnvelope(b)
	if env.Source != "test-service" {
		t.Errorf("Source = %q, want test-service", env.Source)
	}
}

func TestUnmarshalEnvelopeInvalidJSON(t *testing.T) {
	_, err := events.UnmarshalEnvelope([]byte("not-json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestMarshalEnvelopePayloadRoundtrip(t *testing.T) {
	type inner struct {
		PatientID string `json:"patient_id"`
	}

	b, err := events.MarshalEnvelope("test", "c1", "p1", "svc", nil, inner{PatientID: "p123"})
	if err != nil {
		t.Fatal(err)
	}

	env, err := events.UnmarshalEnvelope(b)
	if err != nil {
		t.Fatal(err)
	}

	var got inner
	if err := json.Unmarshal(env.Payload, &got); err != nil {
		t.Fatalf("decode inner payload: %v", err)
	}
	if got.PatientID != "p123" {
		t.Errorf("PatientID = %q, want p123", got.PatientID)
	}
}

func TestMarshalEnvelopeTimestamp(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	b, _ := events.MarshalEnvelope("t", "", "", "s", nil, struct{}{})
	after := time.Now().UTC().Add(time.Second)

	env, _ := events.UnmarshalEnvelope(b)
	if env.Timestamp.Before(before) || env.Timestamp.After(after) {
		t.Errorf("Timestamp %v not in expected range [%v, %v]", env.Timestamp, before, after)
	}
}
