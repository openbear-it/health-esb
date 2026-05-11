package observability

import (
	"log/slog"
	"os"
)

// NewLogger creates a structured slog logger with the given service name and level.
func NewLogger(serviceName, level string) *slog.Logger {
	lvl := slog.LevelInfo
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler).With("service", serviceName)
}
