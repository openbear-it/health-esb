package transformer

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Transformer transforms a raw JSON payload from one schema to another.
type Transformer interface {
	// Name returns the unique identifier used in route configuration.
	// An empty string ("") identifies the PassthroughTransformer.
	Name() string
	// Transform converts the input JSON payload and returns the result.
	Transform(payload json.RawMessage) (json.RawMessage, error)
}

// Registry holds a set of named Transformers.
// All methods are safe for concurrent use.
type Registry struct {
	mu     sync.RWMutex
	byName map[string]Transformer
}

// NewRegistry creates an empty Registry and pre-registers the
// PassthroughTransformer (name "").
func NewRegistry() *Registry {
	r := &Registry{byName: make(map[string]Transformer)}
	r.Register(PassthroughTransformer{})
	return r
}

// Register adds t to the registry, overwriting any previous transformer with
// the same name.
func (r *Registry) Register(t Transformer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byName[t.Name()] = t
}

// Get returns the Transformer registered under name and true if found,
// otherwise nil and false.
func (r *Registry) Get(name string) (Transformer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byName[name]
	return t, ok
}

// MustGet returns the Transformer registered under name, or panics if not
// found. Useful for startup-time assertions.
func (r *Registry) MustGet(name string) Transformer {
	t, ok := r.Get(name)
	if !ok {
		panic(fmt.Sprintf("transformer %q not registered", name))
	}
	return t
}

// ─── Built-in transformers ────────────────────────────────────────────────────

// PassthroughTransformer returns the payload unchanged.
// It is always registered under name "" (empty string).
type PassthroughTransformer struct{}

func (PassthroughTransformer) Name() string { return "" }

func (PassthroughTransformer) Transform(payload json.RawMessage) (json.RawMessage, error) {
	return payload, nil
}
