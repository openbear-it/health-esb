package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	nc "github.com/nats-io/nats.go"
)

// RouterConfig holds configuration for the Watermill router.
type RouterConfig struct {
	ServiceName string
	Logger      watermill.LoggerAdapter
}

// NewRouter creates a preconfigured Watermill router with standard middleware.
func NewRouter(cfg RouterConfig) (*message.Router, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = watermill.NewSlogLogger(slog.Default())
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, fmt.Errorf("create router: %w", err)
	}

	router.AddMiddleware(
		middleware.CorrelationID,
		middleware.Recoverer,
		RetryMiddleware(logger),
	)

	return router, nil
}

// RetryMiddleware returns a Watermill retry middleware with exponential backoff.
func RetryMiddleware(logger watermill.LoggerAdapter) message.HandlerMiddleware {
	return middleware.Retry{
		MaxRetries:      5,
		InitialInterval: time.Second,
		Multiplier:      2,
		Logger:          logger,
	}.Middleware
}

// NewPublisher creates a new NATS JetStream publisher.
func NewPublisher(natsURL string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	opts := []nc.Option{nc.RetryOnFailedConnect(true), nc.MaxReconnects(-1)}
	pub, err := wmnats.NewPublisher(
		wmnats.PublisherConfig{
			URL:         natsURL,
			NatsOptions: opts,
			Marshaler:   &wmnats.NATSMarshaler{},
			JetStream: wmnats.JetStreamConfig{
				AutoProvision: true,
			},
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create nats publisher: %w", err)
	}
	return pub, nil
}

// NewSubscriber creates a new NATS JetStream subscriber.
func NewSubscriber(natsURL, consumerGroup string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	opts := []nc.Option{nc.RetryOnFailedConnect(true), nc.MaxReconnects(-1)}
	sub, err := wmnats.NewSubscriber(
		wmnats.SubscriberConfig{
			URL:              natsURL,
			QueueGroupPrefix: consumerGroup,
			NatsOptions:     opts,
			Unmarshaler:     &wmnats.NATSMarshaler{},
			JetStream: wmnats.JetStreamConfig{
				AutoProvision: true,
				DurablePrefix: consumerGroup,
			},
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create nats subscriber: %w", err)
	}
	return sub, nil
}

// AddPoisonQueue adds a dead-letter queue handler to the router for a given topic.
func AddPoisonQueue(router *message.Router, pub message.Publisher, topic string) {
	pq, err := middleware.PoisonQueue(pub, topic+".dlq")
	if err != nil {
		return
	}
	router.AddMiddleware(pq)
}

// RunRouter starts the router and blocks until ctx is cancelled.
func RunRouter(ctx context.Context, router *message.Router) error {
	return router.Run(ctx)
}
