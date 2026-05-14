package messaging_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
)

// buildMsg creates a Watermill message whose payload is a valid MessageEnvelope
// with the given id. A zero-value id produces an envelope with ID="".
func buildMsg(t *testing.T, id string) *message.Message {
	t.Helper()
	env := events.MessageEnvelope{
		ID:      id,
		Version: "1.0",
		Type:    "test.event",
		Payload: json.RawMessage(`{}`),
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return message.NewMessage(watermill.NewUUID(), b)
}

// buildRawMsg creates a message whose payload is plain JSON (not an envelope).
func buildRawMsg() *message.Message {
	return message.NewMessage(watermill.NewUUID(), []byte(`{"foo":"bar"}`))
}

// noopHandler is a HandlerFunc that always acks and returns nothing.
func noopHandler(msg *message.Message) ([]*message.Message, error) {
	return nil, nil
}

// countingHandler counts how many times it is called.
func countingHandler(count *int) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		*count++
		return nil, nil
	}
}

func applyMiddleware(mw message.HandlerMiddleware, fn message.HandlerFunc) message.HandlerFunc {
	return mw(fn)
}

func TestInMemoryIdempotencyStore_HasAdd(t *testing.T) {
	store := messaging.NewInMemoryIdempotencyStore()

	if store.Has("x") {
		t.Fatal("fresh store must not contain 'x'")
	}

	store.Add("x", time.Hour)
	if !store.Has("x") {
		t.Fatal("store must contain 'x' after Add")
	}
}

func TestInMemoryIdempotencyStore_TTLExpiry(t *testing.T) {
	store := messaging.NewInMemoryIdempotencyStore()
	store.Add("y", time.Millisecond)

	time.Sleep(5 * time.Millisecond)

	if store.Has("y") {
		t.Fatal("expired entry must not be reported as present")
	}
}

func TestInMemoryIdempotencyStore_ResetTTL(t *testing.T) {
	store := messaging.NewInMemoryIdempotencyStore()
	store.Add("z", time.Millisecond)
	store.Add("z", time.Hour) // reset to a long TTL

	time.Sleep(5 * time.Millisecond)

	if !store.Has("z") {
		t.Fatal("reset entry must still be present")
	}
}

func TestIdempotencyMiddleware_FirstPassAllowed(t *testing.T) {
	count := 0
	mw := messaging.IdempotencyMiddleware(messaging.IdempotencyConfig{
		TTL: time.Minute,
	})
	handler := applyMiddleware(mw, countingHandler(&count))

	msg := buildMsg(t, "id-001")
	msg.SetContext(t.Context())

	if _, err := handler(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("handler called %d times, want 1", count)
	}
}

func TestIdempotencyMiddleware_DuplicateDropped(t *testing.T) {
	count := 0
	store := messaging.NewInMemoryIdempotencyStore()
	mw := messaging.IdempotencyMiddleware(messaging.IdempotencyConfig{
		Store: store,
		TTL:   time.Minute,
	})
	handler := applyMiddleware(mw, countingHandler(&count))

	msg1 := buildMsg(t, "dup-id")
	msg1.SetContext(t.Context())
	msg2 := buildMsg(t, "dup-id")
	msg2.SetContext(t.Context())

	if _, err := handler(msg1); err != nil {
		t.Fatalf("first pass error: %v", err)
	}
	if _, err := handler(msg2); err != nil {
		t.Fatalf("second pass error: %v", err)
	}

	if count != 1 {
		t.Errorf("handler called %d times, want 1 (duplicate must be dropped)", count)
	}
}

func TestIdempotencyMiddleware_AfterTTLAllowedAgain(t *testing.T) {
	count := 0
	store := messaging.NewInMemoryIdempotencyStore()
	mw := messaging.IdempotencyMiddleware(messaging.IdempotencyConfig{
		Store: store,
		TTL:   time.Millisecond,
	})
	handler := applyMiddleware(mw, countingHandler(&count))

	msg1 := buildMsg(t, "ttl-id")
	msg1.SetContext(t.Context())
	if _, err := handler(msg1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond) // let TTL expire

	msg2 := buildMsg(t, "ttl-id")
	msg2.SetContext(t.Context())
	if _, err := handler(msg2); err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Errorf("handler called %d times, want 2 after TTL expiry", count)
	}
}

func TestIdempotencyMiddleware_NoEnvelopePassesThrough(t *testing.T) {
	count := 0
	mw := messaging.IdempotencyMiddleware(messaging.IdempotencyConfig{})
	handler := applyMiddleware(mw, countingHandler(&count))

	msg := buildRawMsg()
	msg.SetContext(t.Context())

	if _, err := handler(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("handler called %d times for raw payload, want 1", count)
	}
}

func TestIdempotencyMiddleware_EmptyIDPassesThrough(t *testing.T) {
	count := 0
	mw := messaging.IdempotencyMiddleware(messaging.IdempotencyConfig{})
	handler := applyMiddleware(mw, countingHandler(&count))

	msg := buildMsg(t, "")
	msg.SetContext(t.Context())

	if _, err := handler(msg); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("handler called %d times for empty ID, want 1", count)
	}
}

func TestIdempotencyMiddleware_CustomStore(t *testing.T) {
	custom := &mockStore{seen: map[string]bool{}}
	mw := messaging.IdempotencyMiddleware(messaging.IdempotencyConfig{
		Store: custom,
		TTL:   time.Minute,
	})
	handler := applyMiddleware(mw, noopHandler)

	msg := buildMsg(t, "custom-id")
	msg.SetContext(t.Context())

	if _, err := handler(msg); err != nil {
		t.Fatal(err)
	}

	if !custom.seen["custom-id"] {
		t.Error("custom store must have seen custom-id")
	}
}

// mockStore is a minimal IdempotencyStore for testing custom store injection.
type mockStore struct {
	seen map[string]bool
}

func (m *mockStore) Has(id string) bool             { return m.seen[id] }
func (m *mockStore) Add(id string, _ time.Duration) { m.seen[id] = true }
