package transformer

import (
	"encoding/json"
	"fmt"

	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/fhir"
)

const NameHL7ToFHIR = "hl7_to_fhir"

// FhirObservationTransformer converts a LabResultPayload JSON into a FHIR
// Observation JSON. It is registered under the name "hl7_to_fhir".
type FhirObservationTransformer struct{}

func (FhirObservationTransformer) Name() string { return NameHL7ToFHIR }

// Transform expects payload to be a JSON-encoded events.LabResultPayload and
// returns a JSON-encoded fhir.Observation. The FHIR Observation ID is derived
// from the PatientID field since no correlationID is available at this layer.
func (FhirObservationTransformer) Transform(payload json.RawMessage) (json.RawMessage, error) {
	var lab events.LabResultPayload
	if err := json.Unmarshal(payload, &lab); err != nil {
		return nil, fmt.Errorf("fhir transformer: decode lab payload: %w", err)
	}

	obs := fhir.FromLabResult(lab.PatientID, lab)
	out, err := fhir.ToJSON(obs)
	if err != nil {
		return nil, fmt.Errorf("fhir transformer: encode observation: %w", err)
	}
	return out, nil
}
