package transformer_test

import (
	"encoding/json"
	"testing"

	"github.com/openbear-it/health-esb/internal/transformer"
)

func TestNewRegistry_HasPassthrough(t *testing.T) {
	r := transformer.NewRegistry()
	pt, ok := r.Get("")
	if !ok {
		t.Fatal("passthrough transformer must be present after NewRegistry")
	}
	if pt.Name() != "" {
		t.Errorf("passthrough Name() = %q, want empty string", pt.Name())
	}
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := transformer.NewRegistry()
	r.Register(transformer.FhirObservationTransformer{})

	got, ok := r.Get(transformer.NameHL7ToFHIR)
	if !ok {
		t.Fatalf("expected %q to be registered", transformer.NameHL7ToFHIR)
	}
	if got.Name() != transformer.NameHL7ToFHIR {
		t.Errorf("Name() = %q, want %q", got.Name(), transformer.NameHL7ToFHIR)
	}
}

func TestRegistry_GetMissing(t *testing.T) {
	r := transformer.NewRegistry()
	_, ok := r.Get("no-such-transformer")
	if ok {
		t.Fatal("expected Get to return false for unregistered transformer")
	}
}

func TestRegistry_MustGet_Panics(t *testing.T) {
	r := transformer.NewRegistry()
	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("expected MustGet to panic for unregistered transformer")
		}
	}()
	r.MustGet("missing")
}

func TestRegistry_OverwriteTransformer(t *testing.T) {
	type stubTransformer struct{ transformer.PassthroughTransformer }

	r := transformer.NewRegistry()
	r.Register(transformer.PassthroughTransformer{}) // register same name again
	_, ok := r.Get("")
	if !ok {
		t.Fatal("overwritten passthrough must still be present")
	}
}

func TestPassthroughTransformer(t *testing.T) {
	pt := transformer.PassthroughTransformer{}
	if pt.Name() != "" {
		t.Errorf("Name() = %q, want empty string", pt.Name())
	}

	input := json.RawMessage(`{"key":"value"}`)
	out, err := pt.Transform(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(input) {
		t.Errorf("output = %s, want %s", out, input)
	}
}

func TestFhirObservationTransformer_ValidPayload(t *testing.T) {
	r := transformer.NewRegistry()
	r.Register(transformer.FhirObservationTransformer{})

	tr, _ := r.Get(transformer.NameHL7ToFHIR)

	lab := `{
		"patient_id": "p-001",
		"test_name": "Hemoglobin",
		"value": 14.2,
		"unit": "g/dL",
		"reference_hi": 17.5,
		"reference_lo": 12.0,
		"abnormal": false
	}`

	out, err := tr.Transform(json.RawMessage(lab))
	if err != nil {
		t.Fatalf("Transform error: %v", err)
	}

	var obs map[string]any
	if err := json.Unmarshal(out, &obs); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if obs["resourceType"] != "Observation" {
		t.Errorf("resourceType = %v, want Observation", obs["resourceType"])
	}
	if obs["status"] != "final" {
		t.Errorf("status = %v, want final", obs["status"])
	}
}

func TestFhirObservationTransformer_InvalidPayload(t *testing.T) {
	tr := transformer.FhirObservationTransformer{}
	_, err := tr.Transform(json.RawMessage(`not-json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON payload, got nil")
	}
}

func TestFhirObservationTransformer_Name(t *testing.T) {
	tr := transformer.FhirObservationTransformer{}
	if tr.Name() != transformer.NameHL7ToFHIR {
		t.Errorf("Name() = %q, want %q", tr.Name(), transformer.NameHL7ToFHIR)
	}
}
