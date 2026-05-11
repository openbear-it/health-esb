package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gin-gonic/gin"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
)

// sseBroker manages fan-out of events to multiple SSE clients.
type sseBroker struct {
	clients   map[chan string]struct{}
	subscribe chan chan string
	close     chan chan string
	broadcast chan string
	mu        sync.Mutex
}

func newSSEBroker() *sseBroker {
	return &sseBroker{
		clients:   make(map[chan string]struct{}),
		subscribe: make(chan chan string, 8),
		close:     make(chan chan string, 8),
		broadcast: make(chan string, 64),
	}
}

func (b *sseBroker) run() {
	for {
		select {
		case c := <-b.subscribe:
			b.mu.Lock()
			b.clients[c] = struct{}{}
			b.mu.Unlock()
		case c := <-b.close:
			b.mu.Lock()
			delete(b.clients, c)
			b.mu.Unlock()
		case msg := <-b.broadcast:
			b.mu.Lock()
			for c := range b.clients {
				select {
				case c <- msg:
				default:
				}
			}
			b.mu.Unlock()
		}
	}
}

func (b *sseBroker) send(evt *events.Event) {
	raw, _ := json.Marshal(evt)
	select {
	case b.broadcast <- string(raw):
	default:
	}
}

func handleSSE(broker *sseBroker) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")

		ch := make(chan string, 8)
		broker.subscribe <- ch
		defer func() { broker.close <- ch }()

		ctx := c.Request.Context()
		c.Stream(func(w io.Writer) bool {
			select {
			case msg := <-ch:
				c.SSEvent("event", msg)
				return true
			case <-ctx.Done():
				return false
			}
		})
	}
}

func forwardToSSE(sub message.Subscriber, broker *sseBroker, logger *slog.Logger) {
	topics := []string{
		events.TopicPatientAdmitted,
		events.TopicLabResultCreated,
		events.TopicFHIRDocumentCreated,
		events.TopicNotificationSent,
	}

	for _, topic := range topics {
		msgs, err := sub.Subscribe(nil, topic)
		if err != nil {
			logger.Warn("subscribe failed", "topic", topic, "error", err)
			continue
		}
		go func(t string, ch <-chan *message.Message) {
			for msg := range ch {
				evt, err := messaging.DecodeEvent(msg)
				if err != nil {
					logger.Warn("decode failed", "topic", t, "error", err)
					msg.Nack()
					continue
				}
				broker.send(evt)
				msg.Ack()
				logger.Debug("forwarded to SSE",
					"event_type", evt.Type,
					"correlation_id", evt.CorrelationID,
				)
			}
		}(topic, msgs)
	}
}

// prometheusMiddleware is intentionally unused — promhttp is wired directly in main.go.
