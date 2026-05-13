package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
	"github.com/openbear-it/health-esb/internal/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─── Built-in simulator ───────────────────────────────────────────────────────

type simulatorCtl struct {
	mu      sync.RWMutex
	enabled bool
	rate    float64 // events per second (0.1–20)
}

func (s *simulatorCtl) get() (bool, float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled, s.rate
}

func (s *simulatorCtl) set(enabled bool, rate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rate < 0.1 {
		rate = 0.1
	}
	if rate > 20 {
		rate = 20
	}
	s.enabled = enabled
	s.rate = rate
}

// ─── Chaos control ────────────────────────────────────────────────────────────

type chaosMode string

const (
	chaosModeNone   chaosMode = ""
	chaosModePoison chaosMode = "poison"
	chaosDrop       chaosMode = "drop"
)

type serviceChaos struct {
	Mode      chaosMode `json:"mode"`
	ErrorRate float64   `json:"error_rate"` // 0–1
}

type chaosCtl struct {
	mu       sync.RWMutex
	services map[string]*serviceChaos
}

func newChaosCtl() *chaosCtl {
	return &chaosCtl{
		services: map[string]*serviceChaos{
			"adt-service":          {Mode: chaosModeNone, ErrorRate: 0},
			"lab-service":          {Mode: chaosModeNone, ErrorRate: 0},
			"fhir-bridge":          {Mode: chaosModeNone, ErrorRate: 0},
			"notification-service": {Mode: chaosModeNone, ErrorRate: 0},
			"audit-service":        {Mode: chaosModeNone, ErrorRate: 0},
		},
	}
}

func (c *chaosCtl) get(svc string) serviceChaos {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.services[svc]; ok {
		return *v
	}
	return serviceChaos{}
}

func (c *chaosCtl) set(svc string, mode chaosMode, errorRate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.services[svc]; !ok {
		return
	}
	c.services[svc] = &serviceChaos{Mode: mode, ErrorRate: errorRate}
}

func (c *chaosCtl) status() map[string]serviceChaos {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]serviceChaos, len(c.services))
	for k, v := range c.services {
		out[k] = *v
	}
	return out
}

func (c *chaosCtl) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.services {
		c.services[k] = &serviceChaos{Mode: chaosModeNone, ErrorRate: 0}
	}
}

// serviceInputTopic is the primary topic to inject chaos messages into per service.
var serviceInputTopic = map[string]string{
	"adt-service":          events.TopicCommandPatientAdmit,
	"lab-service":          events.TopicPatientAdmitted,
	"fhir-bridge":          events.TopicLabResultCreated,
	"notification-service": events.TopicPatientAdmitted,
	"audit-service":        events.TopicPatientAdmitted,
}

// injectPoison publishes a malformed message to a topic.
// Watermill will retry it 5 times then route it to the DLQ.
func injectPoison(pub message.Publisher, topic string) error {
	msg := message.NewMessage(uuid.New().String(), []byte(`{"__chaos":"poison"}`))
	msg.Metadata.Set("correlation_id", uuid.New().String())
	msg.Metadata.Set("event_type", topic)
	return pub.Publish(topic, msg)
}

// ─── Synthetic data helpers ───────────────────────────────────────────────────

var (
	firstNames = []string{"Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry", "Irene", "Jack"}
	lastNames  = []string{"Smith", "Jones", "Brown", "Wilson", "Taylor", "Davis", "Clark", "Hall", "Moore", "Lee"}
	wards      = []string{"ICU", "Cardiology", "Oncology", "Pediatrics", "Emergency", "Surgery", "Neurology", "Orthopedics"}
)

func randPatient() (pid, first, last, dob, ward string) {
	pid = fmt.Sprintf("P%04d", rand.Intn(9999))
	first = firstNames[rand.Intn(len(firstNames))]
	last = lastNames[rand.Intn(len(lastNames))]
	year := 1940 + rand.Intn(65)
	dob = fmt.Sprintf("%04d-%02d-%02d", year, 1+rand.Intn(12), 1+rand.Intn(28))
	ward = wards[rand.Intn(len(wards))]
	return
}

// ─── Built-in simulator goroutine ────────────────────────────────────────────

var (
	alertSeverities = []string{"low", "medium", "high", "critical"}
	alertCategories = []string{"vital", "lab", "medication"}
	alertMessages   = []string{
		"SpO2 dropped below threshold",
		"Heart rate outside normal range",
		"Blood pressure critically high",
		"Glucose level abnormal",
		"Temperature elevated",
	}
	labTests = []struct {
		name, unit    string
		lo, hi, scale float64
	}{
		{"Hemoglobin", "g/dL", 12.0, 17.5, 20.0},
		{"White Blood Cells", "10^3/uL", 4.5, 11.0, 15.0},
		{"Platelets", "10^3/uL", 150, 400, 500},
		{"Glucose", "mg/dL", 70, 100, 300},
		{"Creatinine", "mg/dL", 0.6, 1.2, 2.0},
	}
)

func runBuiltinSimulator(ctx context.Context, pub message.Publisher, ctl *simulatorCtl, chaos *chaosCtl, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastRate float64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			enabled, rate := ctl.get()
			if !enabled {
				continue
			}
			if rate != lastRate {
				ticker.Reset(time.Duration(float64(time.Second) / rate))
				lastRate = rate
			}

			// Apply chaos for adt-service (first consumer of admission commands)
			adtChaos := chaos.get("adt-service")
			if adtChaos.Mode == chaosModePoison && rand.Float64() < adtChaos.ErrorRate {
				_ = injectPoison(pub, events.TopicCommandPatientAdmit)
				continue
			}
			if adtChaos.Mode == chaosDrop && rand.Float64() < adtChaos.ErrorRate {
				continue
			}

			// Apply continuous chaos for downstream services: inject poison on their input topics
			for svc, topic := range serviceInputTopic {
				if svc == "adt-service" {
					continue // already handled above
				}
				sc := chaos.get(svc)
				if sc.Mode == chaosModePoison && rand.Float64() < sc.ErrorRate {
					_ = injectPoison(pub, topic)
				}
			}

			pid, first, last, dob, ward := randPatient()
			correlationID := uuid.New().String()

			// Mix of event types: 70% admission, 10% discharge, 10% transfer, 5% lab, 5% alert
			r := rand.Float64()
			switch {
			case r < 0.70:
				// Patient admission (triggers fan-out through ADT → Lab → FHIR → Notify)
				evt, err := events.New(events.TopicCommandPatientAdmit, "gateway-sim", correlationID,
					events.PatientAdmitPayload{
						PatientID:   pid,
						FirstName:   first,
						LastName:    last,
						DateOfBirth: dob,
						Ward:        ward,
					})
				if err != nil {
					continue
				}
				if err := messaging.Publish(pub, events.TopicCommandPatientAdmit, evt); err != nil {
					logger.Error("builtin-sim publish admission", "error", err)
				}

			case r < 0.80:
				// Patient discharge
				evt, err := events.New(events.TopicPatientDischarged, "gateway-sim", correlationID,
					events.PatientDischargedPayload{
						PatientID:   pid,
						FirstName:   first,
						LastName:    last,
						Ward:        ward,
						DischargeAt: time.Now().UTC().Format(time.RFC3339),
						Reason:      []string{"recovered", "transferred", "self-discharge"}[rand.Intn(3)],
					})
				if err != nil {
					continue
				}
				if err := messaging.Publish(pub, events.TopicPatientDischarged, evt); err != nil {
					logger.Error("builtin-sim publish discharge", "error", err)
				}

			case r < 0.90:
				// Patient transfer
				from := wards[rand.Intn(len(wards))]
				to := wards[rand.Intn(len(wards))]
				if from == to && len(wards) > 1 {
					to = wards[(rand.Intn(len(wards)-1)+1+func() int {
						for i, w := range wards {
							if w == from {
								return i
							}
						}
						return 0
					}())%len(wards)]
				}
				evt, err := events.New(events.TopicPatientTransferred, "gateway-sim", correlationID,
					events.PatientTransferPayload{
						PatientID: pid,
						FirstName: first,
						LastName:  last,
						FromWard:  from,
						ToWard:    to,
						Reason:    "clinical decision",
					})
				if err != nil {
					continue
				}
				if err := messaging.Publish(pub, events.TopicPatientTransferred, evt); err != nil {
					logger.Error("builtin-sim publish transfer", "error", err)
				}

			case r < 0.95:
				// Lab result
				lt := labTests[rand.Intn(len(labTests))]
				value := lt.lo*0.5 + rand.Float64()*lt.scale
				evt, err := events.New(events.TopicLabResultCreated, "gateway-sim", correlationID,
					events.LabResultPayload{
						PatientID:   pid,
						TestName:    lt.name,
						Value:       value,
						Unit:        lt.unit,
						ReferenceLo: lt.lo,
						ReferenceHi: lt.hi,
						Abnormal:    value < lt.lo || value > lt.hi,
					})
				if err != nil {
					continue
				}
				if err := messaging.Publish(pub, events.TopicLabResultCreated, evt); err != nil {
					logger.Error("builtin-sim publish lab", "error", err)
				}

			default:
				// Alert
				sev := alertSeverities[rand.Intn(len(alertSeverities))]
				evt, err := events.New(events.TopicAlertCreated, "gateway-sim", correlationID,
					events.AlertPayload{
						PatientID: pid,
						Severity:  sev,
						Category:  alertCategories[rand.Intn(len(alertCategories))],
						Message:   alertMessages[rand.Intn(len(alertMessages))],
						Value:     70 + rand.Float64()*60,
						Threshold: 95,
					})
				if err != nil {
					continue
				}
				if err := messaging.Publish(pub, events.TopicAlertCreated, evt); err != nil {
					logger.Error("builtin-sim publish alert", "error", err)
				}
			}
		}
	}
}

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

	simCtl := &simulatorCtl{enabled: false, rate: 1}
	chaosControl := newChaosCtl()

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go runBuiltinSimulator(ctx, pub, simCtl, chaosControl, logger)

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(corsMiddleware())
	r.Use(requestLogger(logger))

	// Patient events
	r.POST("/admissions", handleAdmission(pub, metrics, logger))
	r.POST("/discharges", handleDischarge(pub, metrics, logger))
	r.POST("/transfers", handleTransfer(pub, metrics, logger))

	// Lab & clinical
	r.POST("/lab-results", handleLabResult(pub, metrics, logger))
	r.POST("/alerts", handleAlert(pub, metrics, logger))

	// Simulator control
	r.GET("/simulator/status", handleSimulatorStatus(simCtl))
	r.POST("/simulator/control", handleSimulatorControl(simCtl))

	// Chaos control
	r.GET("/chaos/status", handleChaosStatus(chaosControl))
	r.POST("/chaos", handleChaosSet(pub, chaosControl))
	r.DELETE("/chaos", handleChaosReset(chaosControl))

	// Infrastructure
	r.GET("/events/stream", handleSSE(sseBroker))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: r,
	}

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

// ─── Request handlers ─────────────────────────────────────────────────────────

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
		logger.Info("event published", "event_type", topic, "correlation_id", correlationID)
		c.JSON(http.StatusAccepted, gin.H{"correlation_id": correlationID, "event_id": evt.ID})
	}
}

// DischargeRequest is the REST payload for a patient discharge.
type DischargeRequest struct {
	PatientID string `json:"patient_id" binding:"required"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Ward      string `json:"ward"`
	Reason    string `json:"reason"` // recovered | transferred | deceased | self-discharge
}

func handleDischarge(pub message.Publisher, m *observability.Metrics, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		topic := events.TopicPatientDischarged

		var req DischargeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if req.Reason == "" {
			req.Reason = "recovered"
		}

		correlationID := uuid.New().String()
		evt, err := events.New(topic, "gateway", correlationID, events.PatientDischargedPayload{
			PatientID:   req.PatientID,
			FirstName:   req.FirstName,
			LastName:    req.LastName,
			Ward:        req.Ward,
			DischargeAt: time.Now().UTC().Format(time.RFC3339),
			Reason:      req.Reason,
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

// TransferRequest is the REST payload for a patient transfer between wards.
type TransferRequest struct {
	PatientID string `json:"patient_id" binding:"required"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	FromWard  string `json:"from_ward" binding:"required"`
	ToWard    string `json:"to_ward" binding:"required"`
	Reason    string `json:"reason"`
}

func handleTransfer(pub message.Publisher, m *observability.Metrics, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		topic := events.TopicPatientTransferred

		var req TransferRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		correlationID := uuid.New().String()
		evt, err := events.New(topic, "gateway", correlationID, events.PatientTransferPayload{
			PatientID: req.PatientID,
			FirstName: req.FirstName,
			LastName:  req.LastName,
			FromWard:  req.FromWard,
			ToWard:    req.ToWard,
			Reason:    req.Reason,
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

// AlertRequest is the incoming REST payload for a clinical alert.
type AlertRequest struct {
	PatientID string  `json:"patient_id" binding:"required"`
	Severity  string  `json:"severity" binding:"required"` // low | medium | high | critical
	Category  string  `json:"category" binding:"required"` // vital | lab | medication | system
	Message   string  `json:"message" binding:"required"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
}

func handleAlert(pub message.Publisher, m *observability.Metrics, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		topic := events.TopicAlertCreated

		var req AlertRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		correlationID := uuid.New().String()
		evt, err := events.New(topic, "gateway", correlationID, events.AlertPayload{
			PatientID: req.PatientID,
			Severity:  req.Severity,
			Category:  req.Category,
			Message:   req.Message,
			Value:     req.Value,
			Threshold: req.Threshold,
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

// ─── Simulator control handlers ───────────────────────────────────────────────

func handleSimulatorStatus(ctl *simulatorCtl) gin.HandlerFunc {
	return func(c *gin.Context) {
		enabled, rate := ctl.get()
		c.JSON(http.StatusOK, gin.H{"enabled": enabled, "rate": rate})
	}
}

type simulatorControlRequest struct {
	Enabled bool    `json:"enabled"`
	Rate    float64 `json:"rate"`
}

func handleSimulatorControl(ctl *simulatorCtl) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req simulatorControlRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ctl.set(req.Enabled, req.Rate)
		enabled, rate := ctl.get()
		c.JSON(http.StatusOK, gin.H{"enabled": enabled, "rate": rate})
	}
}

// ─── Chaos control handlers ───────────────────────────────────────────────────

func handleChaosStatus(ctl *chaosCtl) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, ctl.status())
	}
}

type chaosSetRequest struct {
	Service   string    `json:"service" binding:"required"`
	Mode      chaosMode `json:"mode"`       // "" | "poison" | "drop"
	ErrorRate float64   `json:"error_rate"` // 0–1
}

func handleChaosSet(pub message.Publisher, ctl *chaosCtl) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req chaosSetRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if req.ErrorRate < 0 {
			req.ErrorRate = 0
		}
		if req.ErrorRate > 1 {
			req.ErrorRate = 1
		}
		ctl.set(req.Service, req.Mode, req.ErrorRate)

		// Immediately inject a poison pill when 100% poison is requested.
		if req.Mode == chaosModePoison && req.ErrorRate >= 1 {
			if topic, ok := serviceInputTopic[req.Service]; ok {
				_ = injectPoison(pub, topic)
			}
		}

		c.JSON(http.StatusOK, gin.H{"service": req.Service, "mode": req.Mode, "error_rate": req.ErrorRate})
	}
}

func handleChaosReset(ctl *chaosCtl) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctl.reset()
		c.JSON(http.StatusOK, gin.H{"status": "reset"})
	}
}

// ─── Middleware ───────────────────────────────────────────────────────────────

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
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
