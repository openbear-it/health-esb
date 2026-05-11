# health-esb

**A production-ready demo of a healthcare integration backbone built with [Watermill](https://watermill.io).**

Healthcare systems generate a constant stream of clinical events — admissions, lab results, diagnostic documents, notifications. This project shows how to wire all of them together using an **event-driven architecture** where services are fully decoupled, resilient to failure, and trivially scalable.

Repository: <https://github.com/openbear-it/health-esb>

---

## Why Watermill?

Most event-driven Go code ends up with the same boilerplate repeated in every service: connect to the broker, decode the message, retry on failure, route bad messages to a dead-letter queue, correlate traces across services. **Watermill eliminates all of that.**

| Concern | Hand-rolled code | With Watermill |
|---|---|---|
| Broker connection | Manual NATS/Kafka client | `wmnats.NewPublisher` / `NewSubscriber` |
| Retry with backoff | Custom goroutine + timer | `middleware.Retry{MaxRetries: 5, ...}` |
| Dead-letter queue | Custom per-topic logic | `middleware.PoisonQueue(pub, topic)` |
| Correlation ID | Manual header propagation | `middleware.CorrelationID` |
| Panic recovery | `recover()` in every handler | `middleware.Recoverer` |
| Broker swap | Rewrite all handlers | Change one constructor |

In this project every Go service is a thin wrapper around a Watermill router. The router handles the lifecycle; the developer writes only the business logic.

---

## Architecture

```
 ╔══════════════════════════════════════════════════════════════════╗
 ║                       NATS JetStream                            ║
 ║                  (durable streams + consumer groups)            ║
 ╚══════════════════════════════════════════════════════════════════╝
        ▲  │              │              │              │
  REST  │  │              │              │              │
  POST  │  ▼              ▼              ▼              ▼
 ┌──────┴──────┐  ┌────────────┐  ┌──────────┐  ┌─────────────┐
 │   gateway   │  │adt-service │  │lab-service│  │ fhir-bridge │
 │  REST + SSE │  │            │  │           │  │             │
 └─────────────┘  └────────────┘  └──────────┘  └─────────────┘
        │ SSE                           │              │
        │              ┌────────────────┘              │
        │              ▼                               ▼
        │  ┌───────────────────────┐   ┌───────────────────────┐
        │  │ notification-service  │   │    audit-service       │
        │  └───────────────────────┘   │  (listens ALL topics)  │
        │                              └───────────────────────┘
        ▼
 ┌─────────────┐
 │  dashboard  │  React — live event stream via SSE
 └─────────────┘
```

---

## Event Flow (step by step)

A single patient admission triggers a cascade of events across every service:

```
① Client calls  POST /admissions  on the gateway
        │
        ▼
② gateway publishes  command.patient.admit
   ┌─────────────────────────────────────────────┐
   │ Event {                                      │
   │   id:            "uuid-...",                 │
   │   type:          "command.patient.admit",    │
   │   correlationID: "uuid-...",  ◄─ propagated  │
   │   source:        "gateway",                  │
   │   payload:       PatientAdmitPayload{...}    │
   │ }                                            │
   └─────────────────────────────────────────────┘
        │                    │
        ▼                    ▼
③ adt-service          audit-service
  consumes               logs event
  command.patient.admit
        │
        ▼
④ adt-service publishes  patient.admitted
        │
        ├──► audit-service        (logs event)
        ├──► notification-service (sends admission email)
        └──► lab-service
                │
                ▼
⑤ lab-service runs simulated tests, publishes  lab.result.created
   (one event per test: HbA1c, CBC, CRP, …)
        │
        ├──► audit-service        (logs event)
        ├──► notification-service (sends SMS if value is abnormal)
        └──► fhir-bridge
                │
                ▼
⑥ fhir-bridge wraps the result in a FHIR Observation resource,
  publishes  fhir.document.created
        │
        └──► audit-service        (logs event)

⑦ gateway SSE broker forwards every topic to the dashboard
  in real time.
```

### Topics

| Topic | Producer | Consumers |
|---|---|---|
| `command.patient.admit` | gateway | adt-service, audit-service |
| `patient.admitted` | adt-service | lab-service, notification-service, audit-service |
| `lab.result.created` | lab-service | fhir-bridge, notification-service, audit-service |
| `fhir.document.created` | fhir-bridge | audit-service |
| `notification.sent` | notification-service | audit-service |
| `*.dlq` | Watermill middleware | (manual inspection) |

---

## How Watermill works in this project

### Router + Middleware stack

Every service calls `messaging.NewRouter()` which builds a Watermill router preconfigured with:

```
CorrelationID  →  ensures the same correlationID flows through the whole chain
Recoverer      →  catches panics, nacks the message (triggers retry)
Retry          →  5 attempts with exponential backoff (1s → 2s → 4s → 8s → 16s)
PoisonQueue    →  after 5 failures routes to <topic>.dlq
```

```go
// internal/messaging/router.go  (simplified)
router, _ := message.NewRouter(message.RouterConfig{}, wmLogger)
router.AddMiddleware(
    middleware.CorrelationID,
    middleware.Recoverer,
    middleware.Retry{
        MaxRetries:      5,
        InitialInterval: time.Second,
        Multiplier:      2,
        Logger:          wmLogger,
    }.Middleware,
)

poisonMiddleware, _ := middleware.PoisonQueue(publisher, topic+".dlq")
router.AddMiddleware(poisonMiddleware)
```

### Handler definition

Each service adds exactly one handler per topic it consumes:

```go
// apps/adt-service/main.go  (simplified)
router.AddHandler(
    "adt.admit",                          // handler name (unique)
    events.TopicCommandPatientAdmit,      // input topic
    subscriber,
    events.TopicPatientAdmitted,          // output topic
    publisher,
    handleAdmit,
)
```

The handler receives a `*message.Message` and returns `[]*message.Message` to publish:

```go
func handleAdmit(msg *message.Message) ([]*message.Message, error) {
    evt, _ := messaging.DecodeEvent(msg)
    payload, _ := events.Decode[events.PatientAdmitPayload](evt)

    admitted := events.New(events.TypePatientAdmitted, "adt-service",
        evt.CorrelationID, events.PatientAdmittedPayload{...})

    return messaging.ToMessages(admitted)
}
```

### NATS JetStream — durable consumers

The subscriber uses `QueueGroupPrefix` so that multiple replicas of the same service share a consumer group. NATS delivers each message to exactly one replica:

```go
// internal/messaging/router.go
wmnats.SubscriberConfig{
    JetStream: wmnats.JetStreamConfig{
        AutoProvision: true,
        DurablePrefix: consumerGroup,   // e.g. "lab-service"
    },
    QueueGroupPrefix: consumerGroup,
}
```

Scale `lab-service` to 10 replicas → all 10 share one consumer group → automatic load balancing with zero configuration.

---

## Services

| Service | Port | Role |
|---|---|---|
| `gateway` | 8080 | REST entry point. Accepts `POST /admissions` and `POST /lab-results`, publishes events to NATS. Streams all events to the dashboard via `GET /events/stream` (SSE). Exposes `GET /metrics`. |
| `adt-service` | — | Admission Discharge Transfer. Processes `command.patient.admit`, enriches data, emits `patient.admitted`. |
| `lab-service` | — | Simulates a laboratory. For each admitted patient generates one event per test (HbA1c, CBC, CRP, Glucose, Creatinine) with random values. Flags abnormal results. |
| `fhir-bridge` | — | Healthcare interoperability. Converts lab results to [FHIR R4 Observation](https://www.hl7.org/fhir/observation.html) JSON and publishes them. |
| `notification-service` | — | Sends simulated email (admission) and SMS (abnormal results). |
| `audit-service` | 8081 | Subscribes to every topic. Keeps an in-memory audit log queryable at `GET /audit`. |
| `simulator` | — | Generates a synthetic `POST /admissions` every 2 seconds. Used for demos. |
| `dashboard` | 80 | React + Vite + Recharts. Connects to the SSE stream. Shows per-service counters, live event table, throughput chart, DLQ monitor. |

---

## Repository structure

```
health-esb/
├── .github/
│   └── workflows/
│       ├── build-images.yml   ← multi-platform Docker builds (amd64 + arm64)
│       └── go-ci.yml          ← build, test, format check
├── apps/
│   ├── gateway/               REST API + SSE broker
│   ├── adt-service/           Admission processing
│   ├── lab-service/           Lab result simulation
│   ├── fhir-bridge/           FHIR Observation generation
│   ├── audit-service/         All-event audit log
│   ├── notification-service/  Email & SMS simulation
│   ├── simulator/             Traffic generator
│   └── dashboard/             React live dashboard
├── internal/
│   ├── events/                Canonical Event type, topic constants, payloads
│   ├── messaging/             Watermill router factory + pub/sub helpers
│   ├── config/                Env-based config (PORT, NATS_URL, LOG_LEVEL …)
│   ├── observability/         slog logger, Prometheus metrics, OTel tracing
│   ├── fhir/                  FHIR Observation builder
│   └── hl7/                   Minimal HL7 v2 segment parser
├── deployments/
│   ├── docker/                Dockerfiles (multi-stage, multi-platform)
│   └── k8s/                   Kubernetes manifests for every component
├── Makefile
├── go.mod                     Root module: github.com/openbear-it/health-esb
└── go.work                    Go workspace (root + all service modules)
```

---

## Quick start (local, no Docker)

```bash
# 1. Start NATS JetStream
make run-nats

# 2. In separate terminals, run each service
cd apps/gateway            && go run .
cd apps/adt-service        && go run .
cd apps/lab-service        && go run .
cd apps/fhir-bridge        && go run .
cd apps/audit-service      && go run .
cd apps/notification-service && go run .

# 3. Start the traffic simulator
make simulator

# 4. Watch the SSE stream
curl -N http://localhost:8080/events/stream

# 5. Start the dashboard
cd apps/dashboard && npm install && npm run dev
# open http://localhost:5173
```

---

## Kubernetes deployment

Images are published to `ghcr.io/openbear-it/health-esb/<service>` by the GitHub Actions workflow on every push to `main`.

```bash
# Deploy all manifests in dependency order
make k8s-deploy

# Check rollout status
make k8s-status

# Tail logs for a specific service
make k8s-logs SERVICE=lab-service

# Remove everything
make k8s-delete
```

### Scaling

```bash
kubectl scale deployment lab-service -n health-esb --replicas=5
# All 5 pods share the "lab-service" NATS consumer group automatically.
```

---

## GitHub Actions

### `build-images.yml`

Triggered on: push to `main`, version tags (`v*.*.*`), pull requests.

- **Matrix build**: one job per service (gateway, adt-service, lab-service, fhir-bridge, audit-service, notification-service, simulator, dashboard)
- **Platforms**: `linux/amd64` + `linux/arm64` via Docker Buildx + QEMU
- **Registry**: `ghcr.io/openbear-it/health-esb/<service>`
- **Tags**:
  - Push to `main` → `:main`, `:sha-<short>`  + `:latest`
  - Tag `v1.2.3` → `:1.2.3`, `:1.2`
  - Pull request → `:pr-<n>` (not pushed)
- **Cache**: GitHub Actions cache per service for fast rebuilds

### `go-ci.yml`

Triggered on: push to `main`, pull requests.

- Builds root module + all 7 Go services
- Runs `go test ./...` across all modules
- Enforces `gofmt` formatting

---

## Configuration

All services read from environment variables:

| Variable | Default | Description |
|---|---|---|
| `SERVICE_NAME` | set per-service | Identifier used in logs and metrics |
| `PORT` | `8080` | HTTP listen port |
| `NATS_URL` | `nats://localhost:4222` | NATS JetStream endpoint |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `OTLP_ENDPOINT` | `http://localhost:4318` | OpenTelemetry collector (OTLP HTTP) |

---

## Observability

### Prometheus metrics  (`GET /metrics` on gateway and audit-service)

Each service registers:
- `healthesb_<svc>_messages_processed_total`
- `healthesb_<svc>_messages_failed_total`
- `healthesb_<svc>_message_processing_duration_seconds`
- `healthesb_<svc>_retry_total`
- `healthesb_<svc>_dlq_total`

Prometheus discovers pods automatically via annotation `prometheus.io/scrape: "true"`.

### Distributed tracing

Set `OTLP_ENDPOINT` to point to any OpenTelemetry-compatible collector (Jaeger, Tempo, etc.). The `correlationID` in every event maps directly to the trace context.

---

## Adding a new service

1. Create the module:
   ```bash
   mkdir -p apps/my-service && cd apps/my-service
   go mod init github.com/openbear-it/health-esb/apps/my-service
   ```
2. Add the workspace entry: `go work use ./apps/my-service`
3. Add the replace directive in your `go.mod`:
   ```
   require github.com/openbear-it/health-esb v0.0.0
   replace github.com/openbear-it/health-esb => ../../
   ```
4. Write your handler using the shared `messaging` package (copy any existing service as template)
5. Add a `Dockerfile` in `deployments/docker/`
6. Add a K8s manifest in `deployments/k8s/`
7. Add the service to the matrix in `.github/workflows/build-images.yml`

The broker connection, retry, DLQ, correlation ID, and metrics are all provided by `internal/messaging` — you write only the business logic.

---

## Dead-letter queue

When a handler fails 5 consecutive times the message is automatically routed to `<topic>.dlq`:

```
lab.result.created   →  (after 5 retries)  →  lab.result.created.dlq
patient.admitted     →  (after 5 retries)  →  patient.admitted.dlq
```

The dashboard DLQ panel shows the count of messages in each dead-letter topic. To reprocess a DLQ, publish its messages back to the original topic.
