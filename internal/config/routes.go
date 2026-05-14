package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultRoutesConfigPath = "config/routes.yaml"

// RetryPolicy defines retry behaviour for a route.
type RetryPolicy struct {
	MaxRetries      int           `yaml:"max_retries"`
	InitialInterval time.Duration `yaml:"initial_interval"`
	Multiplier      float64       `yaml:"multiplier"`
}

// RouteConfig describes a single message route between two topics.
type RouteConfig struct {
	Name        string      `yaml:"name"`
	InputTopic  string      `yaml:"input_topic"`
	OutputTopic string      `yaml:"output_topic"`
	Handler     string      `yaml:"handler"`
	Transformer string      `yaml:"transformer"`
	Enabled     bool        `yaml:"enabled"`
	RetryPolicy RetryPolicy `yaml:"retry_policy"`
}

// Validate returns all validation errors for this route in a single call.
// An empty slice means the route is valid.
func (r RouteConfig) Validate() []error {
	var errs []error

	if r.Name == "" {
		errs = append(errs, errors.New("name is required"))
	}
	if r.InputTopic == "" {
		errs = append(errs, fmt.Errorf("route %q: input_topic is required", r.Name))
	}
	if r.OutputTopic == "" {
		errs = append(errs, fmt.Errorf("route %q: output_topic is required", r.Name))
	}
	if r.Handler == "" {
		errs = append(errs, fmt.Errorf("route %q: handler is required", r.Name))
	}
	if r.RetryPolicy.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("route %q: retry_policy.max_retries must be >= 0", r.Name))
	}
	if r.RetryPolicy.Multiplier < 0 {
		errs = append(errs, fmt.Errorf("route %q: retry_policy.multiplier must be >= 0", r.Name))
	}

	return errs
}

// routesFile is the on-disk representation of the YAML file.
type routesFile struct {
	Routes []RouteConfig `yaml:"routes"`
}

// LoadRoutes reads a YAML file from the path provided and returns the parsed
// routes. If path is empty, the value of the ROUTES_CONFIG_PATH environment
// variable is used; if that is also empty, the default path
// "config/routes.yaml" is used.
//
// Validation is not performed here — call Validate on each RouteConfig if
// you need to enforce constraints at load time.
func LoadRoutes(path string) ([]RouteConfig, error) {
	if path == "" {
		path = os.Getenv("ROUTES_CONFIG_PATH")
	}
	if path == "" {
		path = defaultRoutesConfigPath
	}

	f, err := os.Open(path) // #nosec G304 — path comes from operator config
	if err != nil {
		return nil, fmt.Errorf("open routes config %q: %w", path, err)
	}
	defer f.Close()

	var rf routesFile
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&rf); err != nil {
		return nil, fmt.Errorf("decode routes config %q: %w", path, err)
	}

	return rf.Routes, nil
}

// ValidateAll aggregates validation errors from all routes.
// Returns nil if every route is valid.
func ValidateAll(routes []RouteConfig) error {
	var all []error
	for _, r := range routes {
		all = append(all, r.Validate()...)
	}
	return errors.Join(all...)
}
