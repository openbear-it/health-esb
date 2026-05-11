package fhir

import (
	"encoding/json"
	"time"

	"github.com/openbear-it/health-esb/internal/events"
)

// Observation represents a simplified FHIR Observation resource.
type Observation struct {
	ResourceType string       `json:"resourceType"`
	ID           string       `json:"id"`
	Status       string       `json:"status"`
	Code         CodeableConcept `json:"code"`
	Subject      Reference    `json:"subject"`
	EffectiveDateTime string  `json:"effectiveDateTime"`
	ValueQuantity *Quantity   `json:"valueQuantity,omitempty"`
}

// CodeableConcept is a simplified FHIR CodeableConcept.
type CodeableConcept struct {
	Text string `json:"text"`
}

// Reference is a simplified FHIR Reference.
type Reference struct {
	Reference string `json:"reference"`
}

// Quantity is a simplified FHIR Quantity.
type Quantity struct {
	Value  float64 `json:"value"`
	Unit   string  `json:"unit"`
	System string  `json:"system,omitempty"`
}

// FromLabResult converts a LabResultPayload into a FHIR Observation.
func FromLabResult(correlationID string, lab events.LabResultPayload) Observation {
	return Observation{
		ResourceType: "Observation",
		ID:           correlationID,
		Status:       "final",
		Code:         CodeableConcept{Text: lab.TestName},
		Subject:      Reference{Reference: "Patient/" + lab.PatientID},
		EffectiveDateTime: time.Now().UTC().Format(time.RFC3339),
		ValueQuantity: &Quantity{
			Value:  lab.Value,
			Unit:   lab.Unit,
			System: "http://unitsofmeasure.org",
		},
	}
}

// ToJSON serialises an Observation to JSON bytes.
func ToJSON(obs Observation) (json.RawMessage, error) {
	return json.Marshal(obs)
}
