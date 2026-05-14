package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
)

func TestDLQRequeueHandler_MethodNotAllowed(t *testing.T) {
	h := dlqRequeueHandler("amqp://unused", "user", "pass", nil, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dlq/requeue", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestDLQRequeueHandler_Unauthorized(t *testing.T) {
	h := dlqRequeueHandler("amqp://unused", "admin", "secret", nil, nil)
	tests := []struct {
		name string
		user string
		pass string
		want int
	}{
		{"no credentials", "", "", http.StatusUnauthorized},
		{"wrong password", "admin", "wrong", http.StatusUnauthorized},
		{"wrong user", "root", "secret", http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/dlq/requeue", bytes.NewBufferString(`{}`))
			if tc.user != "" || tc.pass != "" {
				req.SetBasicAuth(tc.user, tc.pass)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("expected %d, got %d", tc.want, rec.Code)
			}
		})
	}
}

func TestDLQRequeueHandler_BadBody(t *testing.T) {
	h := dlqRequeueHandler("amqp://unused", "u", "p", nil, nil)

	tests := []struct {
		name string
		body string
		want int
	}{
		{"invalid json", "not-json", http.StatusBadRequest},
		{"missing topic", `{"limit":5}`, http.StatusBadRequest},
		{"topic without -dlq suffix", `{"topic":"some-topic","limit":5}`, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/dlq/requeue", bytes.NewBufferString(tc.body))
			req.SetBasicAuth("u", "p")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("expected %d, got %d", tc.want, rec.Code)
			}
		})
	}
}

func TestDLQRequeueHandler_BrokerError(t *testing.T) {
	// With an invalid AMQP URL the subscriber creation should fail,
	// returning 500 InternalServerError.
	nopLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := dlqRequeueHandler("amqp://127.0.0.1:1", "u", "p", watermill.NopLogger{}, nopLogger)

	body, _ := json.Marshal(requeueRequest{Topic: "lab-result-created-dlq", Limit: 5})
	req := httptest.NewRequest(http.MethodPost, "/dlq/requeue", bytes.NewReader(body))
	req.SetBasicAuth("u", "p")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}
