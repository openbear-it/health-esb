package config

import (
	"context"
	"log/slog"
	"time"

	"github.com/fsnotify/fsnotify"
)

const debounceDuration = 200 * time.Millisecond

// WatchRoutes watches the YAML file at path and sends the reloaded
// []RouteConfig on the returned channel whenever the file changes.
//
// Events are debounced: multiple filesystem notifications within a 200ms
// window are coalesced into a single reload. This avoids duplicate loads on
// rapid saves (e.g. editor swap-file patterns).
//
// The watcher stops and the channel is closed when ctx is cancelled.
// Any error during reload is logged but does not stop the watcher.
func WatchRoutes(ctx context.Context, path string, logger *slog.Logger) (<-chan []RouteConfig, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err := watcher.Add(path); err != nil {
		_ = watcher.Close()
		return nil, err
	}

	ch := make(chan []RouteConfig, 1)

	go func() {
		defer watcher.Close()
		defer close(ch)

		var debounceTimer *time.Timer

		for {
			select {
			case <-ctx.Done():
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return

			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// React only to write/create events; renames (some editors use
				// atomic-save via rename) are included via Create.
				if !event.Has(fsnotify.Write) && !event.Has(fsnotify.Create) {
					continue
				}
				// Reset the debounce timer on each qualifying event.
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.AfterFunc(debounceDuration, func() {
					routes, err := LoadRoutes(path)
					if err != nil {
						if logger != nil {
							logger.Warn("route hot-reload: failed to load config",
								"path", path,
								"error", err,
							)
						}
						return
					}
					if err := ValidateAll(routes); err != nil {
						if logger != nil {
							logger.Warn("route hot-reload: config validation failed",
								"path", path,
								"error", err,
							)
						}
						return
					}
					// Non-blocking send: if the consumer is slow, skip this
					// reload rather than blocking the watcher goroutine.
					select {
					case ch <- routes:
						if logger != nil {
							logger.Info("route hot-reload: config reloaded",
								"path", path,
								"count", len(routes),
							)
						}
					default:
						if logger != nil {
							logger.Warn("route hot-reload: channel full, skipping reload", "path", path)
						}
					}
				})

			case watchErr, ok := <-watcher.Errors:
				if !ok {
					return
				}
				if logger != nil {
					logger.Error("route hot-reload: watcher error", "error", watchErr)
				}
			}
		}
	}()

	return ch, nil
}
