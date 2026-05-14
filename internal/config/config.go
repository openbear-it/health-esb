package config

import (
	"os"
	"strconv"
)

// Config holds common configuration for all services.
type Config struct {
	ServiceName  string
	Port         int
	AMQPUrl      string
	LogLevel     string
	OTLPEndpoint string
	DLQUser      string
	DLQPassword  string
}

// Load reads configuration from environment variables, applying defaults.
func Load(serviceName string) Config {
	return Config{
		ServiceName:  getEnv("SERVICE_NAME", serviceName),
		Port:         getEnvInt("PORT", 8080),
		AMQPUrl:      getEnv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		LogLevel:     getEnv("LOG_LEVEL", "info"),
		OTLPEndpoint: getEnv("OTLP_ENDPOINT", "http://localhost:4318"),
		DLQUser:      getEnv("DLQ_USER", "admin"),
		DLQPassword:  getEnv("DLQ_PASSWORD", ""),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
