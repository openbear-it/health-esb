package messaging

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmamqp "github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
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
		IdempotencyMiddleware(IdempotencyConfig{Logger: logger}),
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

// NewPublisher creates a new RabbitMQ publisher.
// Each topic is mapped to a fanout exchange of the same name, so every
// subscriber queue bound to that exchange receives all messages.
func NewPublisher(amqpURL string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	cfg := wmamqp.NewDurablePubSubConfig(amqpURL, nil)
	pub, err := wmamqp.NewPublisher(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create amqp publisher: %w", err)
	}
	return pub, nil
}

// NewSubscriber creates a new RabbitMQ subscriber.
// consumerGroup is used to derive a unique durable queue name per service,
// ensuring competing-consumer semantics within a group and independent
// fan-out across groups.
func NewSubscriber(amqpURL, consumerGroup string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	cfg := wmamqp.NewDurablePubSubConfig(
		amqpURL,
		wmamqp.GenerateQueueNameTopicNameWithSuffix(consumerGroup),
	)
	sub, err := wmamqp.NewSubscriber(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create amqp subscriber: %w", err)
	}
	return sub, nil
}

// AddPoisonQueue attaches a dead-letter queue middleware to a specific handler.
// Each handler routes its failed messages to <topic>-dlq, keeping DLQs isolated.
func AddPoisonQueue(handler *message.Handler, pub message.Publisher, topic string) {
	pq, err := middleware.PoisonQueue(pub, topic+"-dlq")
	if err != nil {
		return
	}
	handler.AddMiddleware(pq)
}
