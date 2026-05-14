package messaging

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/transformer"
)

// SubscriberFactory creates a Subscriber for the given consumer group.
type SubscriberFactory func(consumerGroup string) (message.Subscriber, error)

// PublisherFactory creates a Publisher.
type PublisherFactory func() (message.Publisher, error)

// HandlerFactory resolves a handler function by name.
type HandlerFactory func(name string) (message.HandlerFunc, bool)

// RouterBuilderConfig holds the wiring for RouterBuilder.
type RouterBuilderConfig struct {
	Logger              watermill.LoggerAdapter
	SubscriberFactory   SubscriberFactory
	PublisherFactory    PublisherFactory
	HandlerFactory      HandlerFactory
	TransformerRegistry *transformer.Registry
}

// RouterBuilder manages the lifecycle of a Watermill router, supporting
// hot-reload from a config.WatchRoutes channel.
type RouterBuilder struct {
	cfg    RouterBuilderConfig
	logger *slog.Logger
}

// NewRouterBuilder creates a RouterBuilder ready for use.
func NewRouterBuilder(cfg RouterBuilderConfig, logger *slog.Logger) *RouterBuilder {
	return &RouterBuilder{cfg: cfg, logger: logger}
}

// Run builds the initial router from routes, starts it, and listens for
// updates on watchCh. On each update it gracefully shuts down the current
// router, rebuilds with the new config, and restarts. Blocks until ctx is
// cancelled.
func (rb *RouterBuilder) Run(ctx context.Context, routes []config.RouteConfig, watchCh <-chan []config.RouteConfig) error {
	current := routes
	for {
		router, pub, err := rb.build(current)
		if err != nil {
			return fmt.Errorf("router builder: initial build: %w", err)
		}

		routerCtx, cancelRouter := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() {
			errCh <- router.Run(routerCtx)
		}()

		select {
		case <-ctx.Done():
			cancelRouter()
			<-errCh
			if pub != nil {
				_ = pub.Close()
			}
			return ctx.Err()

		case err := <-errCh:
			cancelRouter()
			if pub != nil {
				_ = pub.Close()
			}
			return fmt.Errorf("router stopped unexpectedly: %w", err)

		case newRoutes, ok := <-watchCh:
			if !ok {
				// Watcher closed — keep running with current config.
				cancelRouter()
				<-errCh
				if pub != nil {
					_ = pub.Close()
				}
				// Fall back to blocking on context only.
				<-ctx.Done()
				return ctx.Err()
			}
			rb.logDiff(current, newRoutes)
			cancelRouter()
			<-errCh
			if pub != nil {
				_ = pub.Close()
			}
			current = newRoutes
		}
	}
}

// build creates a fresh router+publisher from the given routes slice.
// Only enabled routes are added. Subscribers are created per-route.
func (rb *RouterBuilder) build(routes []config.RouteConfig) (*message.Router, message.Publisher, error) {
	router, err := NewRouter(RouterConfig{Logger: rb.cfg.Logger})
	if err != nil {
		return nil, nil, fmt.Errorf("new router: %w", err)
	}

	pub, err := rb.cfg.PublisherFactory()
	if err != nil {
		return nil, nil, fmt.Errorf("create publisher: %w", err)
	}

	for _, route := range routes {
		if !route.Enabled {
			continue
		}

		handlerFn, ok := rb.cfg.HandlerFactory(route.Handler)
		if !ok {
			if rb.logger != nil {
				rb.logger.Warn("router builder: handler not found, skipping route",
					"route", route.Name,
					"handler", route.Handler,
				)
			}
			continue
		}

		sub, err := rb.cfg.SubscriberFactory(route.Name)
		if err != nil {
			_ = pub.Close()
			return nil, nil, fmt.Errorf("create subscriber for route %q: %w", route.Name, err)
		}

		wrapped := rb.wrapWithTransformer(handlerFn, route.Transformer)

		h := router.AddHandler(route.Name, route.InputTopic, sub, route.OutputTopic, pub, wrapped)
		AddPoisonQueue(h, pub, route.OutputTopic)
	}

	return router, pub, nil
}

// wrapWithTransformer decorates handlerFn so that each outgoing message
// payload is passed through the named transformer before publishing.
// If the transformer is not found it falls back to passthrough.
func (rb *RouterBuilder) wrapWithTransformer(fn message.HandlerFunc, tName string) message.HandlerFunc {
	if rb.cfg.TransformerRegistry == nil {
		return fn
	}
	tr, ok := rb.cfg.TransformerRegistry.Get(tName)
	if !ok {
		// Fall back to passthrough.
		tr, _ = rb.cfg.TransformerRegistry.Get("")
	}
	if tr == nil {
		return fn
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		out, err := fn(msg)
		if err != nil {
			return nil, err
		}
		var transformed []*message.Message
		for _, m := range out {
			payload, tErr := tr.Transform([]byte(m.Payload))
			if tErr != nil {
				return nil, fmt.Errorf("transformer %q: %w", tName, tErr)
			}
			nm := message.NewMessage(m.UUID, message.Payload(payload))
			nm.Metadata = m.Metadata
			transformed = append(transformed, nm)
		}
		return transformed, nil
	}
}

// LogDiff is exported for testing; production code uses logDiff.
func (rb *RouterBuilder) LogDiff(old, next []config.RouteConfig) { rb.logDiff(old, next) }

// routeKey returns a string that uniquely identifies the content of a route
// for diffing purposes.
func routeKey(r config.RouteConfig) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%v",
		r.Name, r.InputTopic, r.OutputTopic, r.Handler, r.Transformer, r.Enabled)
}

// logDiff logs which routes were added, removed, or changed.
func (rb *RouterBuilder) logDiff(old, next []config.RouteConfig) {
	if rb.logger == nil {
		return
	}
	oldMap := make(map[string]string, len(old))
	for _, r := range old {
		oldMap[r.Name] = routeKey(r)
	}
	nextMap := make(map[string]string, len(next))
	for _, r := range next {
		nextMap[r.Name] = routeKey(r)
	}

	for _, r := range next {
		prev, existed := oldMap[r.Name]
		if !existed {
			rb.logger.Info("router hot-reload: route added", "route", r.Name)
		} else if prev != routeKey(r) {
			rb.logger.Info("router hot-reload: route changed", "route", r.Name)
		}
	}
	for _, r := range old {
		if _, still := nextMap[r.Name]; !still {
			rb.logger.Info("router hot-reload: route removed", "route", r.Name)
		}
	}
	rb.logger.Info("router hot-reload: applying new config", "enabled_routes", len(next))
}
