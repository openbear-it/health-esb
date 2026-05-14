package messaging

import (
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/events"
)

const defaultIdempotencyTTL = 10 * time.Minute

// IdempotencyStore is the pluggable backend used to track processed message IDs.
// Implementations must be safe for concurrent use.
//
// Has reports whether the given ID has already been processed.
// Add records the ID as processed; the store may evict it after TTL.
type IdempotencyStore interface {
	Has(id string) bool
	Add(id string, ttl time.Duration)
}

// idempotencyEntry is a single record kept by InMemoryIdempotencyStore.
type idempotencyEntry struct {
	expiresAt time.Time
}

// InMemoryIdempotencyStore is a TTL-based, sync.Map-backed IdempotencyStore.
// Expired entries are evicted lazily on Has/Add calls (no background goroutine).
type InMemoryIdempotencyStore struct {
	m sync.Map
}

// NewInMemoryIdempotencyStore creates an InMemoryIdempotencyStore ready for use.
func NewInMemoryIdempotencyStore() *InMemoryIdempotencyStore {
	return &InMemoryIdempotencyStore{}
}

// Has returns true if id was previously added and has not yet expired.
func (s *InMemoryIdempotencyStore) Has(id string) bool {
	v, ok := s.m.Load(id)
	if !ok {
		return false
	}
	entry := v.(idempotencyEntry)
	if time.Now().After(entry.expiresAt) {
		s.m.Delete(id)
		return false
	}
	return true
}

// Add records id with the given TTL. Calling Add with an already-present id
// resets its expiry.
func (s *InMemoryIdempotencyStore) Add(id string, ttl time.Duration) {
	s.m.Store(id, idempotencyEntry{expiresAt: time.Now().Add(ttl)})
}

// IdempotencyConfig holds options for the idempotency middleware.
type IdempotencyConfig struct {
	// Store is the backend used to detect duplicates. Defaults to a new
	// InMemoryIdempotencyStore when nil.
	Store IdempotencyStore
	// TTL is how long a processed message ID is remembered.
	// Defaults to defaultIdempotencyTTL (10 minutes) when zero.
	TTL time.Duration
	// Logger is used to emit duplicate-detection warnings.
	Logger watermill.LoggerAdapter
}

// IdempotencyMiddleware returns a Watermill HandlerMiddleware that drops
// duplicate messages based on the envelope ID.
//
// The middleware reads the envelope from the Watermill message payload. If the
// envelope ID has already been processed (and has not expired from the store),
// the message is Nack-ed and dropped with a warning log. Otherwise the ID is
// recorded in the store and the next handler is called.
func IdempotencyMiddleware(cfg IdempotencyConfig) message.HandlerMiddleware {
	if cfg.Store == nil {
		cfg.Store = NewInMemoryIdempotencyStore()
	}
	if cfg.TTL == 0 {
		cfg.TTL = defaultIdempotencyTTL
	}
	logger := cfg.Logger
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			env, err := events.UnmarshalEnvelope(msg.Payload)
			if err != nil {
				// Payload is not an envelope; let it pass through untouched.
				return next(msg)
			}

			id := env.ID
			if id == "" {
				// No ID to deduplicate on; let it pass through.
				return next(msg)
			}

			if cfg.Store.Has(id) {
				logger.Info("idempotency: duplicate message dropped", watermill.LogFields{
					"envelope_id":    id,
					"watermill_uuid": msg.UUID,
					"type":           env.Type,
				})
				msg.Nack()
				return nil, nil
			}

			cfg.Store.Add(id, cfg.TTL)
			return next(msg)
		}
	}
}
