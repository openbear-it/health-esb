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

// PatientDischargedPayload is the payload for patient-discharged.
type PatientDischargedPayload struct {
	PatientID   string `json:"patient_id"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	Ward        string `json:"ward"`
	DischargeAt string `json:"discharge_at"`
	Reason      string `json:"reason"` // recovered | transferred | deceased | self-discharge
}

// PatientTransferPayload is the payload for patient-transferred.
type PatientTransferPayload struct {
	PatientID string `json:"patient_id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	FromWard  string `json:"from_ward"`
	ToWard    string `json:"to_ward"`
	Reason    string `json:"reason"`
}

// AlertPayload is the payload for alert-created.
type AlertPayload struct {
	PatientID string  `json:"patient_id"`
	Severity  string  `json:"severity"` // low | medium | high | critical
	Category  string  `json:"category"` // vital | lab | medication | system
	Message   string  `json:"message"`
	Value     float64 `json:"value,omitempty"`
	Threshold float64 `json:"threshold,omitempty"`
}

// NotificationPayload is the payload for notification.sent.
type NotificationPayload struct {
	PatientID string `json:"patient_id"`
	Channel   string `json:"channel"` // email | sms | push
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
