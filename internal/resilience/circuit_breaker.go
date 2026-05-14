package resilience

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// State represents a circuit breaker state.
type State int

const (
	StateClosed   State = iota // normal operation
	StateOpen                  // failing — calls rejected immediately
	StateHalfOpen              // one probe allowed
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when the circuit is open and the call is rejected.
var ErrCircuitOpen = errors.New("circuit breaker open")

// Breaker wraps a function call with circuit breaker logic.
type Breaker interface {
	Call(ctx context.Context, fn func() error) error
	State() State
}

// Config holds circuit breaker settings.
type Config struct {
	// Target is an identifier for this circuit breaker (used in metrics labels).
	Target string
	// Threshold is the number of consecutive failures that trips the circuit.
	// Default: 5.
	Threshold int
	// ResetTimeout is how long the circuit stays open before moving to HalfOpen.
	// Default: 30 seconds.
	ResetTimeout time.Duration
	// Gauge is the Prometheus gauge for circuit breaker state (optional).
	// Labels must be (target, state).
	Gauge *prometheus.GaugeVec
}

func (c *Config) defaults() {
	if c.Threshold <= 0 {
		c.Threshold = 5
	}
	if c.ResetTimeout <= 0 {
		c.ResetTimeout = 30 * time.Second
	}
}

// circuitBreaker is the concrete Breaker implementation.
type circuitBreaker struct {
	mu           sync.Mutex
	cfg          Config
	state        State
	failures     int
	openedAt     time.Time
	probeInFlight bool // true when a HalfOpen probe is running
}

// NewBreaker creates and returns a new circuit breaker.
func NewBreaker(cfg Config) Breaker {
	cfg.defaults()
	cb := &circuitBreaker{cfg: cfg, state: StateClosed}
	cb.reportState()
	return cb
}

func (cb *circuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.currentState()
}

// currentState computes the effective state, advancing Open→HalfOpen after
// the reset timeout. Must be called with cb.mu held.
func (cb *circuitBreaker) currentState() State {
	if cb.state == StateOpen && time.Since(cb.openedAt) >= cb.cfg.ResetTimeout {
		cb.state = StateHalfOpen
		cb.reportStateLocked()
	}
	return cb.state
}

// Call executes fn respecting circuit breaker state.
//   - Closed: calls fn, counts successes/failures.
//   - Open: rejects immediately with ErrCircuitOpen.
//   - HalfOpen: allows one probe; success → Closed, failure → Open again.
func (cb *circuitBreaker) Call(ctx context.Context, fn func() error) error {
	cb.mu.Lock()
	state := cb.currentState()

	switch state {
	case StateOpen:
		cb.mu.Unlock()
		return ErrCircuitOpen
	case StateHalfOpen:
		if cb.probeInFlight {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		cb.probeInFlight = true
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateHalfOpen {
		cb.probeInFlight = false
	}

	if err != nil {
		cb.onFailure()
	} else {
		cb.onSuccess()
	}
	return err
}

func (cb *circuitBreaker) onSuccess() {
	cb.failures = 0
	cb.state = StateClosed
	cb.reportStateLocked()
}

func (cb *circuitBreaker) onFailure() {
	if cb.state == StateHalfOpen {
		cb.trip()
		return
	}
	cb.failures++
	if cb.failures >= cb.cfg.Threshold {
		cb.trip()
	}
}

func (cb *circuitBreaker) trip() {
	cb.state = StateOpen
	cb.openedAt = time.Now()
	cb.failures = 0
	cb.reportStateLocked()
}

// reportState updates the Prometheus gauge. Call without the lock.
func (cb *circuitBreaker) reportState() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.reportStateLocked()
}

// reportStateLocked updates the Prometheus gauge. Must be called with cb.mu held.
func (cb *circuitBreaker) reportStateLocked() {
	g := cb.cfg.Gauge
	if g == nil {
		return
	}
	target := cb.cfg.Target
	states := []State{StateClosed, StateOpen, StateHalfOpen}
	for _, s := range states {
		val := 0.0
		if s == cb.state {
			val = 1.0
		}
		g.WithLabelValues(target, s.String()).Set(val)
	}
}
