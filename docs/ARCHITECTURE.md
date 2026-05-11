# Health ESB — Architecture & Quick Start

A complete demo platform that showcases how to build a modern **event-driven healthcare integration backbone** using:

- **Go 1.22**
- **[Watermill](https://watermill.io)** — event routing & messaging
- **NATS JetStream** — messaging backbone
- **Kubernetes / K3s** — cloud-native deployment
- **FHIR** — healthcare interoperability (simplified)
- **Prometheus + Grafana + OpenTelemetry** — observability

> The goal is to demonstrate: **how easy it is to build healthcare integration systems with Watermill.**

---

## Architecture

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │                          NATS JetStream                              │
 └──────────────────────────────────────────────────────────────────────┘
         │               │              │              │
    ┌────▼────┐    ┌──────▼──────┐  ┌──▼──────┐  ┌───▼────────┐
    │ Gateway │    │ ADT Service │  │  Lab    │  │FHIR Bridge │
    │REST/SSE │    │             │  │ Service │  │            │
    └────┬────┘    └──────┬──────┘  └──┬──────┘  └───┬────────┘
         │               │              │              │
         │    ┌──────────▼──────────────▼──────────────▼────────────┐
         │    │              Notification Service                    │
         │    └──────────────────────────────────────────────────────┘
         │    ┌──────────────────────────────────────────────────────┐
         │    │                   Audit Service                      │
         │    └──────────────────────────────────────────────────────┘
         │
    ┌────▼──────────────────┐
    │     Dashboard (React) │  ← live SSE stream
    └───────────────────────┘
```

---

## Event Flow

```
curl POST /admissions
       │
       ▼
  Gateway ──► command.patient.admit
                    │
                    ▼
              ADT Service ──► patient.admitted
                                    │
                    ┌───────────────┼────────────────┐
                    ▼               ▼                ▼
               Lab Service    Notification      Audit Service
                    │          Service
                    ▼
             lab.result.created
                    │
                    ├──► FHIR Bridge ──► fhir.document.created
                    │
                    └──► Notification ──► notification.sent (if abnormal)
```

---

## Quick Start

### With Docker Compose

```bash
# Start everything
make run

# Open dashboard
open http://localhost:3000

# Send a test admission
make demo-admit

# Watch the event stream
curl -N http://localhost:8080/events/stream
```

### Local development

```bash
# Start NATS
make run-nats

# Build all services
make build

# Run a service
cd apps/gateway && go run .
```

---

## Kubernetes Deployment (K3s)

```bash
make docker
make k8s-deploy
make k8s-status
```

---

## Demo Walkthrough

### 1. Send admission
```bash
make demo-admit
```

### 2. Scale lab-service
```bash
make demo-scale
# → 5 replicas, NATS queue groups distribute events automatically
```

### 3. Chaos test
```bash
make demo-chaos
# → Pod deleted, Watermill retries with exponential backoff
# → After 5 retries: lab.result.created.dlq
```

---

## Why Watermill?

| Feature | Without Watermill | With Watermill |
|---------|------------------|----------------|
| Retry logic | Custom per-handler | `middleware.Retry` |
| DLQ routing | Custom per-topic | `middleware.PoisonQueue` |
| Correlation tracking | Manual | `middleware.CorrelationID` |
| Broker abstraction | Tight coupling | Swap NATS → Kafka in one line |

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_NAME` | service-specific | Service identifier |
| `PORT` | `8080` | HTTP port |
| `NATS_URL` | `nats://localhost:4222` | NATS connection string |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `OTLP_ENDPOINT` | `http://localhost:4318` | OpenTelemetry collector |

---

## Adding a New Service

1. `go mod init github.com/health-esb/apps/my-service`
2. Add `use ./apps/my-service` to `go.work`
3. Copy handler pattern from any existing service
4. Add `Dockerfile` and K8s manifest
5. Add to `docker-compose.yml`
