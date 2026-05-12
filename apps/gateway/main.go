package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
	"github.com/openbear-it/health-esb/internal/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	cfg := config.Load("gateway")
	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	wmLogger := watermill.NewSlogLogger(logger)

	pub, err := messaging.NewPublisher(cfg.AMQPUrl, wmLogger)
	if err != nil {
		logger.Error("failed to create publisher", "error", err)
		return
	}
	defer pub.Close()

	metrics := observability.NewMetrics("gateway")

	// SSE broker: a channel fan-out for connected dashboard clients
	sseBroker := newSSEBroker()
	go sseBroker.run()

	// Subscribe to all events to forward them to SSE clients
	sub, err := messaging.NewSubscriber(cfg.AMQPUrl, "gateway-sse", wmLogger)
	if err != nil {
		logger.Warn("failed to create SSE subscriber", "error", err)
	} else {
		go forwardToSSE(context.Background(), sub, sseBroker, logger)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger(logger))

	r.POST("/admissions", handleAdmission(pub, metrics, logger))
	r.POST("/lab-results", handleLabResult(pub, metrics, logger))
	r.GET("/events/stream", handleSSE(sseBroker))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: r,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("gateway listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// AdmissionRequest is the incoming REST payload for a patient admission.
type AdmissionRequest struct {
	PatientID   string `json:"patient_id" binding:"required"`
	FirstName   string `json:"first_name" binding:"required"`
	LastName    string `json:"last_name" binding:"required"`
	DateOfBirth string `json:"date_of_birth"`
	Ward        string `json:"ward"`
}

func handleAdmission(pub message.Publisher, m *observability.Metrics, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		topic := events.TopicCommandPatientAdmit

		var req AdmissionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		correlationID := uuid.New().String()
		evt, err := events.New(topic, "gateway", correlationID, events.PatientAdmitPayload{
			PatientID:   req.PatientID,
			FirstName:   req.FirstName,
			LastName:    req.LastName,
			DateOfBirth: req.DateOfBirth,
			Ward:        req.Ward,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}

		if err := messaging.Publish(pub, topic, evt); err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			logger.Error("publish failed", "topic", topic, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "publish failed"})
			return
		}

		m.MessagesProcessedTotal.WithLabelValues(topic).Inc()
		m.ProcessingDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
		logger.Info("event published",
			"event_type", topic,
			"correlation_id", correlationID,
			"duration_ms", time.Since(start).Milliseconds(),
		)

		c.JSON(http.StatusAccepted, gin.H{"correlation_id": correlationID, "event_id": evt.ID})
	}
}

// LabResultRequest is the incoming REST payload for a lab result.
type LabResultRequest struct {
	PatientID   string  `json:"patient_id" binding:"required"`
	TestName    string  `json:"test_name" binding:"required"`
	Value       float64 `json:"value"`
	Unit        string  `json:"unit"`
	ReferenceHi float64 `json:"reference_hi"`
	ReferenceLo float64 `json:"reference_lo"`
}

func handleLabResult(pub message.Publisher, m *observability.Metrics, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		topic := events.TopicLabResultCreated

		var req LabResultRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		correlationID := uuid.New().String()
		abnormal := req.Value < req.ReferenceLo || req.Value > req.ReferenceHi
		evt, err := events.New(topic, "gateway", correlationID, events.LabResultPayload{
			PatientID:   req.PatientID,
			TestName:    req.TestName,
			Value:       req.Value,
			Unit:        req.Unit,
			ReferenceHi: req.ReferenceHi,
			ReferenceLo: req.ReferenceLo,
			Abnormal:    abnormal,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}

		if err := messaging.Publish(pub, topic, evt); err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "publish failed"})
			return
		}

		m.MessagesProcessedTotal.WithLabelValues(topic).Inc()
		m.ProcessingDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
		c.JSON(http.StatusAccepted, gin.H{"correlation_id": correlationID, "event_id": evt.ID})
	}
}

func requestLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.Info("request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}
