package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openbear-it/health-esb/internal/config"
)

// writeYAML writes content to a temp file and returns its path.
func writeYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "routes-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	_ = f.Close()
	return f.Name()
}

const validYAML = `
routes:
  - name: patient-admit
    input_topic: command-patient-admit
    output_topic: patient-admitted
    handler: adt_admit
    transformer: ""
    enabled: true
    retry_policy:
      max_retries: 5
      initial_interval: 1s
      multiplier: 2.0
  - name: lab-result-generate
    input_topic: patient-admitted
    output_topic: lab-result-created
    handler: lab_generate_result
    transformer: ""
    enabled: false
    retry_policy:
      max_retries: 3
      initial_interval: 500ms
      multiplier: 1.5
`

func TestLoadRoutes_ValidYAML(t *testing.T) {
	path := writeYAML(t, validYAML)
	routes, err := config.LoadRoutes(path)
	if err != nil {
		t.Fatalf("LoadRoutes: %v", err)
	}
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	tests := []struct {
		idx         int
		name        string
		inputTopic  string
		outputTopic string
		handler     string
		transformer string
		enabled     bool
		maxRetries  int
		interval    time.Duration
		multiplier  float64
	}{
		{
			idx:         0,
			name:        "patient-admit",
			inputTopic:  "command-patient-admit",
			outputTopic: "patient-admitted",
			handler:     "adt_admit",
			transformer: "",
			enabled:     true,
			maxRetries:  5,
			interval:    time.Second,
			multiplier:  2.0,
		},
		{
			idx:         1,
			name:        "lab-result-generate",
			inputTopic:  "patient-admitted",
			outputTopic: "lab-result-created",
			handler:     "lab_generate_result",
			transformer: "",
			enabled:     false,
			maxRetries:  3,
			interval:    500 * time.Millisecond,
			multiplier:  1.5,
		},
	}

	for _, tc := range tests {
		r := routes[tc.idx]
		if r.Name != tc.name {
			t.Errorf("[%d] Name = %q, want %q", tc.idx, r.Name, tc.name)
		}
		if r.InputTopic != tc.inputTopic {
			t.Errorf("[%d] InputTopic = %q, want %q", tc.idx, r.InputTopic, tc.inputTopic)
		}
		if r.OutputTopic != tc.outputTopic {
			t.Errorf("[%d] OutputTopic = %q, want %q", tc.idx, r.OutputTopic, tc.outputTopic)
		}
		if r.Handler != tc.handler {
			t.Errorf("[%d] Handler = %q, want %q", tc.idx, r.Handler, tc.handler)
		}
		if r.Transformer != tc.transformer {
			t.Errorf("[%d] Transformer = %q, want %q", tc.idx, r.Transformer, tc.transformer)
		}
		if r.Enabled != tc.enabled {
			t.Errorf("[%d] Enabled = %v, want %v", tc.idx, r.Enabled, tc.enabled)
		}
		if r.RetryPolicy.MaxRetries != tc.maxRetries {
			t.Errorf("[%d] MaxRetries = %d, want %d", tc.idx, r.RetryPolicy.MaxRetries, tc.maxRetries)
		}
		if r.RetryPolicy.InitialInterval != tc.interval {
			t.Errorf("[%d] InitialInterval = %v, want %v", tc.idx, r.RetryPolicy.InitialInterval, tc.interval)
		}
		if r.RetryPolicy.Multiplier != tc.multiplier {
			t.Errorf("[%d] Multiplier = %v, want %v", tc.idx, r.RetryPolicy.Multiplier, tc.multiplier)
		}
	}
}

func TestLoadRoutes_FileNotFound(t *testing.T) {
	_, err := config.LoadRoutes(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadRoutes_InvalidYAML(t *testing.T) {
	path := writeYAML(t, "routes: [[[invalid")
	_, err := config.LoadRoutes(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestLoadRoutes_UnknownFields(t *testing.T) {
	yaml := `
routes:
  - name: test
    input_topic: a
    output_topic: b
    handler: h
    unknown_field: oops
    enabled: true
`
	path := writeYAML(t, yaml)
	_, err := config.LoadRoutes(path)
	if err == nil {
		t.Fatal("expected error for unknown field, got nil")
	}
}

func TestLoadRoutes_EnvVar(t *testing.T) {
	path := writeYAML(t, validYAML)
	t.Setenv("ROUTES_CONFIG_PATH", path)

	routes, err := config.LoadRoutes("") // empty → use env var
	if err != nil {
		t.Fatalf("LoadRoutes via env: %v", err)
	}
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}
}

func TestValidateAll_Valid(t *testing.T) {
	routes := []config.RouteConfig{
		{
			Name:        "test-route",
			InputTopic:  "in",
			OutputTopic: "out",
			Handler:     "h",
			Enabled:     true,
			RetryPolicy: config.RetryPolicy{MaxRetries: 3, Multiplier: 2.0},
		},
	}
	if err := config.ValidateAll(routes); err != nil {
		t.Errorf("expected no errors, got: %v", err)
	}
}

func TestRouteConfig_Validate_Errors(t *testing.T) {
	tests := []struct {
		name        string
		route       config.RouteConfig
		wantMsgs    []string // substrings expected in combined error message
	}{
		{
			name:     "empty name",
			route:    config.RouteConfig{InputTopic: "in", OutputTopic: "out", Handler: "h"},
			wantMsgs: []string{"name is required"},
		},
		{
			name:     "missing input_topic",
			route:    config.RouteConfig{Name: "r", OutputTopic: "out", Handler: "h"},
			wantMsgs: []string{"input_topic is required"},
		},
		{
			name:     "missing output_topic",
			route:    config.RouteConfig{Name: "r", InputTopic: "in", Handler: "h"},
			wantMsgs: []string{"output_topic is required"},
		},
		{
			name:     "missing handler",
			route:    config.RouteConfig{Name: "r", InputTopic: "in", OutputTopic: "out"},
			wantMsgs: []string{"handler is required"},
		},
		{
			name: "negative max_retries",
			route: config.RouteConfig{
				Name:        "r",
				InputTopic:  "in",
				OutputTopic: "out",
				Handler:     "h",
				RetryPolicy: config.RetryPolicy{MaxRetries: -1},
			},
			wantMsgs: []string{"max_retries must be >= 0"},
		},
		{
			name: "negative multiplier",
			route: config.RouteConfig{
				Name:        "r",
				InputTopic:  "in",
				OutputTopic: "out",
				Handler:     "h",
				RetryPolicy: config.RetryPolicy{Multiplier: -1},
			},
			wantMsgs: []string{"multiplier must be >= 0"},
		},
		{
			name: "multiple errors at once",
			route: config.RouteConfig{
				RetryPolicy: config.RetryPolicy{MaxRetries: -1, Multiplier: -1},
			},
			wantMsgs: []string{"name is required", "input_topic is required", "output_topic is required", "handler is required"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs := tc.route.Validate()
			if len(errs) == 0 {
				t.Fatal("expected validation errors, got none")
			}
			combined := make([]string, len(errs))
			for i, e := range errs {
				combined[i] = e.Error()
			}
			all := strings.Join(combined, "\n")
			for _, msg := range tc.wantMsgs {
				if !strings.Contains(all, msg) {
					t.Errorf("expected error containing %q, got:\n%s", msg, all)
				}
			}
		})
	}
}
