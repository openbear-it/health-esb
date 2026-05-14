package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openbear-it/health-esb/internal/resilience"
)

var errFail = errors.New("failure")

func callN(t *testing.T, cb resilience.Breaker, n int, fn func() error) {
	t.Helper()
	for i := 0; i < n; i++ {
		_ = cb.Call(context.Background(), fn)
	}
}

func TestCircuitBreaker_ClosedByDefault(t *testing.T) {
	cb := resilience.NewBreaker(resilience.Config{Target: "t"})
	if got := cb.State(); got != resilience.StateClosed {
		t.Fatalf("expected Closed, got %v", got)
	}
}

func TestCircuitBreaker_TripsAfterThreshold(t *testing.T) {
	cb := resilience.NewBreaker(resilience.Config{Target: "t", Threshold: 3})
	callN(t, cb, 3, func() error { return errFail })
	if got := cb.State(); got != resilience.StateOpen {
		t.Fatalf("expected Open after 3 failures, got %v", got)
	}
}

func TestCircuitBreaker_RejectsWhenOpen(t *testing.T) {
	cb := resilience.NewBreaker(resilience.Config{Target: "t", Threshold: 1})
	_ = cb.Call(context.Background(), func() error { return errFail })
	err := cb.Call(context.Background(), func() error { return nil })
	if !errors.Is(err, resilience.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	cb := resilience.NewBreaker(resilience.Config{Target: "t", Threshold: 1, ResetTimeout: 10 * time.Millisecond})
	_ = cb.Call(context.Background(), func() error { return errFail })
	time.Sleep(20 * time.Millisecond)
	if got := cb.State(); got != resilience.StateHalfOpen {
		t.Fatalf("expected HalfOpen, got %v", got)
	}
}

func TestCircuitBreaker_ProbeSuccessCloses(t *testing.T) {
	cb := resilience.NewBreaker(resilience.Config{Target: "t", Threshold: 1, ResetTimeout: 10 * time.Millisecond})
	_ = cb.Call(context.Background(), func() error { return errFail })
	time.Sleep(20 * time.Millisecond)

	// Probe succeeds → should close.
	if err := cb.Call(context.Background(), func() error { return nil }); err != nil {
		t.Fatalf("probe call should succeed: %v", err)
	}
	if got := cb.State(); got != resilience.StateClosed {
		t.Fatalf("expected Closed after successful probe, got %v", got)
	}
}

func TestCircuitBreaker_ProbeFailureReopens(t *testing.T) {
	cb := resilience.NewBreaker(resilience.Config{Target: "t", Threshold: 1, ResetTimeout: 10 * time.Millisecond})
	_ = cb.Call(context.Background(), func() error { return errFail })
	time.Sleep(20 * time.Millisecond)

	_ = cb.Call(context.Background(), func() error { return errFail })
	if got := cb.State(); got != resilience.StateOpen {
		t.Fatalf("expected Open after failed probe, got %v", got)
	}
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cb := resilience.NewBreaker(resilience.Config{Target: "t", Threshold: 3})
	callN(t, cb, 2, func() error { return errFail })
	// Success should reset counter.
	_ = cb.Call(context.Background(), func() error { return nil })
	// Two more failures should not trip (counter was reset).
	callN(t, cb, 2, func() error { return errFail })
	if got := cb.State(); got != resilience.StateClosed {
		t.Fatalf("expected Closed (counter reset), got %v", got)
	}
}

func TestCircuitBreaker_HalfOpenRejectsSecondProbe(t *testing.T) {
	// When already in HalfOpen with a probe in flight, second call must be rejected.
	// We can't easily test concurrency in unit tests without goroutines; test that
	// after the first probe runs and fails a second concurrent call before resetting.
	cb := resilience.NewBreaker(resilience.Config{Target: "t", Threshold: 1, ResetTimeout: 10 * time.Millisecond})
	_ = cb.Call(context.Background(), func() error { return errFail })
	time.Sleep(20 * time.Millisecond)

	errCh := make(chan error, 2)
	block := make(chan struct{})
	go func() {
		errCh <- cb.Call(context.Background(), func() error {
			<-block // hold probe in flight
			return nil
		})
	}()
	time.Sleep(5 * time.Millisecond) // let goroutine start
	// Second call while probe in flight must be rejected.
	errCh <- cb.Call(context.Background(), func() error { return nil })
	close(block)

	err1 := <-errCh
	err2 := <-errCh
	// One of the two must be ErrCircuitOpen.
	if !errors.Is(err1, resilience.ErrCircuitOpen) && !errors.Is(err2, resilience.ErrCircuitOpen) {
		t.Fatalf("expected one ErrCircuitOpen from concurrent HalfOpen calls, got %v / %v", err1, err2)
	}
}
