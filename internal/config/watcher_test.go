package config_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/openbear-it/health-esb/internal/config"
)

const watcherTestYAML = `
routes:
  - name: test-route
    input_topic: in
    output_topic: out
    handler: h
    enabled: true
    retry_policy:
      max_retries: 1
      initial_interval: 1s
      multiplier: 2.0
`

const watcherUpdatedYAML = `
routes:
  - name: test-route
    input_topic: in
    output_topic: out-updated
    handler: h
    enabled: true
    retry_policy:
      max_retries: 2
      initial_interval: 1s
      multiplier: 2.0
`

func TestWatchRoutes_DetectsChange(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "routes-watch-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(watcherTestYAML); err != nil {
		t.Fatalf("write initial yaml: %v", err)
	}
	_ = f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := config.WatchRoutes(ctx, f.Name(), nil)
	if err != nil {
		t.Fatalf("WatchRoutes: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := os.WriteFile(f.Name(), []byte(watcherUpdatedYAML), 0o600); err != nil {
		t.Fatalf("overwrite yaml: %v", err)
	}

	select {
	case routes, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		if len(routes) == 0 {
			t.Fatal("expected at least one route")
		}
		if routes[0].OutputTopic != "out-updated" {
			t.Errorf("OutputTopic = %q, want out-updated", routes[0].OutputTopic)
		}
		if routes[0].RetryPolicy.MaxRetries != 2 {
			t.Errorf("MaxRetries = %d, want 2", routes[0].RetryPolicy.MaxRetries)
		}
	case <-ctx.Done():
		t.Fatal("timeout: no reload event received within 5s")
	}
}

func TestWatchRoutes_ContextCancellation(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "routes-cancel-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(watcherTestYAML); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	_ = f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := config.WatchRoutes(ctx, f.Name(), nil)
	if err != nil {
		t.Fatalf("WatchRoutes: %v", err)
	}

	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			select {
			case _, ok2 := <-ch:
				if ok2 {
					t.Fatal("channel not closed after context cancellation")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("channel not closed within 2s after context cancellation")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed within 2s after context cancellation")
	}
}

func TestWatchRoutes_FileNotFound(t *testing.T) {
	ctx := context.Background()
	_, err := config.WatchRoutes(ctx, "/nonexistent/path/routes.yaml", nil)
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
}

func TestWatchRoutes_InvalidYAMLNotSent(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "routes-invalid-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(watcherTestYAML); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	_ = f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, err := config.WatchRoutes(ctx, f.Name(), nil)
	if err != nil {
		t.Fatalf("WatchRoutes: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := os.WriteFile(f.Name(), []byte("routes: [[[invalid"), 0o600); err != nil {
		t.Fatalf("overwrite yaml: %v", err)
	}

	select {
	case routes := <-ch:
		t.Errorf("expected no reload for invalid YAML, got %v", routes)
	case <-ctx.Done():
		// Correct: nothing received.
	}
}
