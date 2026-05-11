package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// PatientAdmitPayload is the payload for command.patient.admit.
type PatientAdmitPayload struct {
	PatientID   string `json:"patient_id"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	DateOfBirth string `json:"date_of_birth"`
	Ward        string `json:"ward"`
}

// LabResultPayload is the payload for lab.result.created.
type LabResultPayload struct {
	PatientID   string  `json:"patient_id"`
	TestName    string  `json:"test_name"`
	Value       float64 `json:"value"`
	Unit        string  `json:"unit"`
	ReferenceHi float64 `json:"reference_hi"`
	ReferenceLo float64 `json:"reference_lo"`
	Abnormal    bool    `json:"abnormal"`
}

// FHIRDocumentPayload is the payload for fhir.document.created.
type FHIRDocumentPayload struct {
	PatientID    string          `json:"patient_id"`
	ResourceType string          `json:"resource_type"`
	Document     json.RawMessage `json:"document"`
}

// NotificationPayload is the payload for notification.sent.
type NotificationPayload struct {
	PatientID string `json:"patient_id"`
	Channel   string `json:"channel"` // email | sms
	Message   string `json:"message"`
}

// New creates a new Event with a generated ID and current timestamp.
func New(eventType, source, correlationID string, payload any) (*Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return &Event{
		ID:            uuid.New().String(),
		Type:          eventType,
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC(),
		Source:        source,
		Payload:       raw,
	}, nil
}

// Decode unmarshals the Event payload into the given target.
func Decode[T any](e *Event) (T, error) {
	var target T
	if err := json.Unmarshal(e.Payload, &target); err != nil {
		return target, fmt.Errorf("decode payload: %w", err)
	}
	return target, nil
}
