package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/openbear-it/health-esb/internal/messaging"
)

const (
	dlqMaxLimit     = 100
	dlqReadTimeout  = 2 * time.Second
	dlqTotalTimeout = 30 * time.Second
)

// requeueRequest is the JSON body for POST /dlq/requeue.
type requeueRequest struct {
	Topic string `json:"topic"`
	Limit int    `json:"limit"`
}

// requeueResponse is the JSON response for POST /dlq/requeue.
type requeueResponse struct {
	Requeued int    `json:"requeued"`
	Topic    string `json:"source_topic"`
}

// dlqRequeueHandler returns an HTTP handler for POST /dlq/requeue.
// It is guarded by HTTP Basic Auth using the provided credentials.
func dlqRequeueHandler(amqpURL, dlqUser, dlqPassword string, wmLogger watermill.LoggerAdapter, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Basic auth guard — constant-time comparison via net/http built-in.
		user, pass, ok := r.BasicAuth()
		if !ok || user != dlqUser || pass != dlqPassword {
			w.Header().Set("WWW-Authenticate", `Basic realm="dlq"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		// Decode body.
		var req requeueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid body: %v", err), http.StatusBadRequest)
			return
		}
		if req.Topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}
		if !strings.HasSuffix(req.Topic, "-dlq") {
			http.Error(w, "topic must end with -dlq", http.StatusBadRequest)
			return
		}
		if req.Limit <= 0 || req.Limit > dlqMaxLimit {
			req.Limit = dlqMaxLimit
		}

		originalTopic := strings.TrimSuffix(req.Topic, "-dlq")

		// Dedicated subscriber for this requeue operation.
		sub, err := messaging.NewSubscriber(amqpURL, "dlq-requeue-"+req.Topic, wmLogger)
		if err != nil {
			logger.Error("dlq requeue: create subscriber", "error", err)
			http.Error(w, "failed to connect to broker", http.StatusInternalServerError)
			return
		}
		defer sub.Close()

		pub, err := messaging.NewPublisher(amqpURL, wmLogger)
		if err != nil {
			logger.Error("dlq requeue: create publisher", "error", err)
			http.Error(w, "failed to connect to broker", http.StatusInternalServerError)
			return
		}
		defer pub.Close()

		ctx, cancel := context.WithTimeout(r.Context(), dlqTotalTimeout)
		defer cancel()

		msgs, err := sub.Subscribe(ctx, req.Topic)
		if err != nil {
			logger.Error("dlq requeue: subscribe", "topic", req.Topic, "error", err)
			http.Error(w, "failed to subscribe to DLQ", http.StatusInternalServerError)
			return
		}

		requeued := 0
		for requeued < req.Limit {
			select {
			case msg, open := <-msgs:
				if !open {
					goto done
				}
				if err := pub.Publish(originalTopic, msg); err != nil {
					logger.Warn("dlq requeue: publish failed", "topic", originalTopic, "msg_uuid", msg.UUID, "error", err)
					msg.Nack()
					continue
				}
				msg.Ack()
				requeued++
				logger.Info("dlq requeue: message requeued",
					"from_dlq", req.Topic,
					"to_topic", originalTopic,
					"msg_uuid", msg.UUID,
				)
			case <-time.After(dlqReadTimeout):
				// No more messages in the DLQ within the timeout window.
				goto done
			case <-ctx.Done():
				goto done
			}
		}

	done:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(requeueResponse{
			Requeued: requeued,
			Topic:    originalTopic,
		})
	}
}
