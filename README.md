# health-esb

**A production-ready demo of a healthcare integration backbone built with [Watermill](https://watermill.io).**

Healthcare systems generate a constant stream of clinical events — admissions, discharges, transfers, lab results, FHIR documents, notifications, alerts. This project shows how to wire all of them together using an **event-driven architecture** where services are fully decoupled, resilient to failure, and trivially scalable.

Repository: <https://github.com/openbear-it/health-esb>

---

## Why Watermill?

Most event-driven Go code ends up with the same boilerplate repeated in every service: connect to the broker, decode the message, retry on failure, route bad messages to a dead-letter queue, correlate traces across services. **Watermill eliminates all of that.**

| Concern | Hand-rolled code | With Watermill |
|---|---|---|
| Broker connection | Manual AMQP client | `wmamqp.NewPublisher` / `NewSubscriber` |
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
 ║                       RabbitMQ (AMQP)                           ║
 ║            (durable fanout exchanges + per-service queues)      ║
 ╚══════════════════════════════════════════════════════════════════╝
        ▲  │              │              │              │
  REST  │  │              │              │              │
  POST  │  ▼              ▼              ▼              ▼
 ┌──────┴──────┐  ┌────────────┐  ┌──────────┐  ┌─────────────┐
 │   gateway   │  │adt-service │  │lab-service│  │ fhir-bridge │
 │  REST + SSE │  │            │  │           │  │             │
 │  + sim-ctl  │  └────────────┘  └──────────┘  └─────────────┘
 │  + chaos    │                       │              │
 └─────────────┘         ┌─────────────┘              │
        │ SSE            ▼                             ▼
        │  ┌───────────────────────┐   ┌───────────────────────┐
        │  │ notification-service  │   │    audit-service       │
        │  └───────────────────────┘   │  (listens ALL topics)  │
        │                              └───────────────────────┘
        ▼
 ┌─────────────┐
 │  dashboard  │  React — SSE stream + full control panel
 └─────────────┘
```

---

## Event Flow (step by step)

```
① Client calls  POST /admissions  (or the built-in simulator fires)
        │
        ▼
② gateway publishes  command-patient-admit
        │
        ├──► adt-service  (processes admission)
        │         │
        │         ▼
        │    publishes  patient-admitted
        │         │
        │         ├──► lab-service
        │         │         │  (runs HbA1c, WBC, Platelets, Glucose, Creatinine)
        │         │         ▼
        │         │    publishes  lab-result-created  (one per test)
        │         │         │
        │         │         ├──► fhir-bridge
        │         │         │         │  (wraps in FHIR R4 Observation)
        │         │         │         ▼
        │         │         │    publishes  fhir-document-created
        │         │         │
        │         │         └──► notification-service  (SMS if abnormal)
        │         │
        │         └──► notification-service  (admission email)
        │
        └──► audit-service  (listens to every topic)

③ Client calls  POST /discharges  →  patient-discharged
④ Client calls  POST /transfers   →  patient-transferred
⑤ Client calls  POST /alerts      →  alert-created

⑦ gateway SSE broker forwards ALL topics to the dashboard in real time.
```

### Topics

| Topic | Producer | Consumers |
|---|---|---|
| `command-patient-admit` | gateway, gateway-sim | adt-service, audit-service |
| `patient-admitted` | adt-service | lab-service, notification-service, audit-service |
| `patient-discharged` | gateway | audit-service |
| `patient-transferred` | gateway | audit-service |
| `lab-result-created` | lab-service | fhir-bridge, notification-service, audit-service |
| `lab-result-validated` | (future) | — |
| `fhir-document-created` | fhir-bridge | audit-service |
| `notification-sent` | notification-service | audit-service |
| `alert-created` | gateway | audit-service |
| `*.dlq` | Watermill middleware | (manual inspection) |

---

## How Watermill works in this project

### Router + Middleware stack

Every service calls `messaging.NewRouter()` which builds a Watermill router preconfigured with:

```
CorrelationID  →  ensures the same correlationID flows through the whole chain
Recoverer      →  catches panics, nacks the message (triggers retry)
Idempotency    →  deduplicates by envelope ID (10 min TTL, pluggable store)
Retry          →  5 attempts with exponential backoff (1s → 2s → 4s → 8s → 16s)
PoisonQueue    →  after 5 failures routes to <topic>-dlq (per-handler)
```

```go
// internal/messaging/router.go  (simplified)
router, _ := message.NewRouter(message.RouterConfig{}, wmLogger)
router.AddMiddleware(
    middleware.CorrelationID,
    middleware.Recoverer,
    messaging.IdempotencyMiddleware(cfg),   // dedup by envelope ID
    middleware.Retry{
        MaxRetries:      5,
        InitialInterval: time.Second,
        Multiplier:      2,
        Logger:          wmLogger,
    }.Middleware,
)
// PoisonQueue registered per-handler:
poisonMiddleware, _ := middleware.PoisonQueue(publisher, topic+"-dlq")
handler.AddMiddleware(poisonMiddleware)
```

### Handler definition

```go
router.AddHandler(
    "adt-handle-admit",
    events.TopicCommandPatientAdmit,
    subscriber,
    events.TopicPatientAdmitted,
    publisher,
    handleAdmit,
)
```

---

## Services

| Service | Port | Role |
|---|---|---|
| `gateway` | 8080 | REST entry point. Exposes all patient/lab/alert endpoints. Built-in simulator with configurable rate. Chaos injection API. Streams all events to the dashboard via SSE. |
| `adt-service` | — | Admission/Discharge/Transfer. Processes `command-patient-admit`, enriches data, emits `patient-admitted`. |
| `lab-service` | — | Simulates a laboratory. For each admitted patient generates one event per test (HbA1c, WBC, Platelets, Glucose, Creatinine) with random values. Flags abnormal results. |
| `fhir-bridge` | — | Healthcare interoperability. Converts lab results to FHIR R4 Observation JSON and publishes them. |
| `notification-service` | — | Sends simulated email (admission) and SMS (abnormal results). |
| `audit-service` | 8081 | Subscribes to every topic. Writes audit records as newline-delimited JSON to `$AUDIT_LOG_PATH` (default `/var/log/health-esb/audit.jsonl`). Daily file rotation, 30-day retention. Query API: `GET /audit?from=<RFC3339>&to=<RFC3339>&type=<event_type>`. DLQ requeue: `POST /dlq/requeue`. |
| `simulator` | — | Standalone traffic generator. Generates a synthetic `POST /admissions` every 2 seconds. Alternative to the gateway built-in simulator. |
| `dashboard` | 80 | React + Vite + Recharts. Full interactive control panel: live stream, charts, simulator control, chaos engineering, manual event injection. |

---

## Dashboard features

The React dashboard connects to the gateway SSE stream and provides:

### Live monitoring
- **Stats row** — per-event-type counters: admissions, discharges, transfers, lab results, FHIR docs, notifications, alerts, DLQ
- **Service pipeline** — per-service event counters with chaos indicator (⚡ icon when a service is disrupted)
- **Throughput chart** — stacked area chart, one area per topic, 60-second rolling window
- **Event distribution** — bar chart, pie chart, combined DLQ + alert panel
- **Live event stream** — scrollable table with per-row badge, filterable by type and free text; click any row to open the **Message Inspector** with full JSON payload

### Simulator control
Control the **built-in gateway simulator** without running the external `simulator` service:

| Control | Description |
|---|---|
| Rate slider | 0.1 – 20 events/second |
| ▶ Start / ■ Stop | Enable or disable the synthetic traffic |
| Burst ×5 / ×10 / ×20 | Instantly set rate to 5, 10, or 20 evt/s |

### Chaos engineering
Inject faults into any downstream service from the dashboard:

| Button | Effect |
|---|---|
| **Poison** | Publishes a malformed message to the service's input topic. Watermill retries 5 times then routes to DLQ. The DLQ counter in the stats row increments. |
| **Drop 50%** | The gateway discards ~50% of messages destined for that service. |
| **Off** | Removes the fault for that service. |
| **Reset all** | Clears all faults at once. |

Services with active chaos show an ⚡ icon in the pipeline view.

### Middleware chain bar

Displayed below the KPI grid: shows the live middleware stack (`CorrelationID → Recoverer → Idempotency → Retry → PoisonQueue`) and active feature badges (Envelope v1.0, OTel Tracing, Circuit Breaker).

### DLQ Requeue panel

Select a DLQ topic and limit, enter Basic Auth credentials, and click **Requeue** to push messages back to the original topic via `POST /dlq/requeue` on the audit-service.

### Audit Query panel

Filter audit records by time range (`from`/`to` datetime pickers) and event type, then click **Query** to stream matching NDJSON records from the audit log via `GET /audit`.

### Manual event injection
Send any event type directly from the dashboard:

| Form | Endpoint | Key fields |
|---|---|---|
| Admission | `POST /admissions` | patient ID, name, DOB, ward |
| Discharge | `POST /discharges` | patient ID, ward, reason (recovered / transferred / deceased / self-discharge) |
| Transfer | `POST /transfers` | patient ID, from-ward, to-ward, reason |
| Lab Result | `POST /lab-results` | patient ID, test selector, value slider (shows ABNORMAL badge when out of range) |
| Alert | `POST /alerts` | patient ID, severity (low/medium/high/critical), category (vital/lab/medication/system), message |

---

## Repository structure

```
health-esb/
├── apps/
│   ├── gateway/               REST API + SSE + built-in simulator + chaos API
│   ├── adt-service/           Admission processing
│   ├── lab-service/           Lab result simulation
│   ├── fhir-bridge/           FHIR Observation generation
│   ├── audit-service/         All-event audit log
│   ├── notification-service/  Email & SMS simulation
│   ├── simulator/             Standalone traffic generator
│   └── dashboard/             React live dashboard (full control panel)
├── internal/
│   ├── events/                Canonical MessageEnvelope, all topic constants, all payloads
│   ├── messaging/             Watermill router factory + middleware chain + RouterBuilder
│   ├── config/                Env config · YAML route config · fsnotify hot-reload watcher
│   ├── observability/         slog logger, Prometheus metrics, OTel tracing
│   ├── resilience/            Circuit breaker (Closed/Open/HalfOpen)
│   ├── transformer/           Transformer interface · Registry · FhirObservationTransformer
│   ├── fhir/                  FHIR Observation builder
│   └── hl7/                   Minimal HL7 v2 segment parser
├── deployments/
│   ├── docker/                Dockerfiles (multi-stage, multi-platform)
│   └── k8s/                   Kubernetes manifests for every component
├── Makefile
├── go.mod
└── go.work
```

---

## Quick start (local, no Docker)

```bash
# 1. Start RabbitMQ
make run-rabbitmq   # or: docker run -d -p 5672:5672 rabbitmq:3-management

# 2. Start each service in separate terminals
cd apps/gateway              && go run .
cd apps/adt-service          && go run .
cd apps/lab-service          && go run .
cd apps/fhir-bridge          && go run .
cd apps/audit-service        && go run .
cd apps/notification-service && go run .

# 3. (Optional) Start the external simulator
make simulator

# 4. Watch the SSE stream
curl -N http://localhost:8080/events/stream

# 5. Start the dashboard
cd apps/dashboard && npm install && npm run dev
# open http://localhost:5173
```

---

## REST API reference

### Patient events

```bash
# Admit a patient
curl -X POST http://localhost:8080/admissions \
  -H 'Content-Type: application/json' \
  -d '{"patient_id":"P001","first_name":"Alice","last_name":"Smith","date_of_birth":"1980-05-10","ward":"ICU"}'

# Discharge a patient
curl -X POST http://localhost:8080/discharges \
  -H 'Content-Type: application/json' \
  -d '{"patient_id":"P001","first_name":"Alice","last_name":"Smith","ward":"ICU","reason":"recovered"}'

# Transfer a patient between wards
curl -X POST http://localhost:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"patient_id":"P001","first_name":"Alice","last_name":"Smith","from_ward":"Emergency","to_ward":"ICU","reason":"stabilized"}'
```

### Lab results

```bash
curl -X POST http://localhost:8080/lab-results \
  -H 'Content-Type: application/json' \
  -d '{"patient_id":"P001","test_name":"Glucose","value":250,"unit":"mg/dL","reference_lo":70,"reference_hi":100}'
```

### Alerts

```bash
curl -X POST http://localhost:8080/alerts \
  -H 'Content-Type: application/json' \
  -d '{"patient_id":"P001","severity":"critical","category":"vital","message":"Heart rate > 180 bpm","value":185,"threshold":130}'
```

### Built-in simulator

```bash
# Start at 2 evt/s
curl -X POST http://localhost:8080/simulator/control \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"rate":2.0}'

# Check status
curl http://localhost:8080/simulator/status

# Stop
curl -X POST http://localhost:8080/simulator/control \
  -H 'Content-Type: application/json' \
  -d '{"enabled":false,"rate":1.0}'
```

### Chaos engineering

```bash
# Inject poison messages into lab-service (→ DLQ buildup)
curl -X POST http://localhost:8080/chaos \
  -H 'Content-Type: application/json' \
  -d '{"service":"lab-service","mode":"poison","error_rate":1.0}'

# Drop 50% of messages going to fhir-bridge
curl -X POST http://localhost:8080/chaos \
  -H 'Content-Type: application/json' \
  -d '{"service":"fhir-bridge","mode":"drop","error_rate":0.5}'

# Check current chaos state
curl http://localhost:8080/chaos/status

# Reset all faults
curl -X DELETE http://localhost:8080/chaos
```

Available services: `adt-service`, `lab-service`, `fhir-bridge`, `notification-service`, `audit-service`  
Available modes: `poison` (→ DLQ), `drop` (silent discard), `""` (off)

### Audit log

```bash
# Stream all records from the current audit.jsonl
curl http://localhost:8081/audit

# Filter by time range and event type
curl "http://localhost:8081/audit?from=2025-01-01T00:00:00Z&to=2025-12-31T23:59:59Z&type=lab-result-created"
```

### DLQ requeue

```bash
curl -X POST http://localhost:8081/dlq/requeue \
  -u admin: \
  -H 'Content-Type: application/json' \
  -d '{"topic":"lab-result-created-dlq","limit":10}'
```

Available DLQ topics follow the pattern `<original-topic>-dlq`.

---

## Kubernetes deployment

```bash
make k8s-deploy     # deploy all manifests in dependency order
make k8s-status     # check rollout status
make k8s-logs SERVICE=lab-service
make k8s-delete     # remove everything
```

### Scaling

```bash
kubectl scale deployment lab-service -n health-esb --replicas=5
# All 5 pods share the "lab-service" AMQP consumer queue automatically.
```

---

## GitHub Actions

### `build-images.yml`

- **Matrix build**: one job per service
- **Platforms**: `linux/amd64` + `linux/arm64` via Docker Buildx + QEMU
- **Registry**: `ghcr.io/openbear-it/health-esb/<service>`
- Tags: `:latest` on `main`, `:v1.2.3` on version tags, `:pr-<n>` on PRs

### `go-ci.yml`

- Builds root module + all 7 Go services
- Runs `go test ./...`
- Enforces `gofmt` formatting

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `SERVICE_NAME` | set per-service | Identifier used in logs and metrics |
| `PORT` | `8080` | HTTP listen port |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ endpoint |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `OTLP_ENDPOINT` | `http://localhost:4318` | OpenTelemetry collector (OTLP HTTP) |
| `ROUTES_CONFIG_PATH` | `config/routes.yaml` | YAML route configuration file |
| `AUDIT_LOG_PATH` | `/var/log/health-esb/audit.jsonl` | Audit NDJSON log file path (audit-service) |
| `DLQ_USER` | `admin` | Basic auth username for `POST /dlq/requeue` |
| `DLQ_PASSWORD` | `""` | Basic auth password for `POST /dlq/requeue` |

---

## Observability

### Prometheus metrics (`GET /metrics` on gateway and audit-service)

Each service registers:
- `healthesb_<svc>_messages_processed_total`
- `healthesb_<svc>_messages_failed_total`
- `healthesb_<svc>_message_processing_duration_seconds`
- `healthesb_<svc>_retry_total`
- `healthesb_<svc>_dlq_total`

Global metrics (no `<svc>` prefix):
- `healthesb_message_e2e_duration_seconds` — end-to-end duration from envelope timestamp to handler completion (labels: `source_service`, `dest_service`, `topic`)
- `healthesb_queue_depth_total` — current RabbitMQ queue depth polled every 15 s (label: `queue_name`)
- `healthesb_circuit_breaker_state` — 1 when the CB is in that state (labels: `target`, `state`)

### Distributed tracing

Set `OTLP_ENDPOINT` to any OpenTelemetry-compatible collector (Jaeger, Tempo, etc.). The W3C `traceparent` is injected into every `MessageEnvelope.Headers` at publish time and reconstructed by `messaging.TracingMiddleware` on the consume side. Each handler produces a child span named `<service>/<topic>/handle`.

---

## Dead-letter queue

When a handler fails 5 consecutive times the message is routed to `<topic>-dlq`:

```
lab-result-created   →  (after 5 retries)  →  lab-result-created-dlq
patient-admitted     →  (after 5 retries)  →  patient-admitted-dlq
```

Trigger this from the dashboard with the **Chaos → Poison** button.  
The DLQ counter in the stats row increments in real time.

### Requeue API

```bash
# Push messages from a DLQ back to the original topic
curl -X POST http://localhost:8081/dlq/requeue \
  -u admin: \
  -H 'Content-Type: application/json' \
  -d '{"topic":"lab-result-created-dlq","limit":10}'
```

Or use the **DLQ Requeue** panel in the dashboard.

---

## Adding a new service

1. Create the module:
   ```bash
   mkdir -p apps/my-service && cd apps/my-service
   go mod init github.com/openbear-it/health-esb/apps/my-service
   ```
2. Add `go work use ./apps/my-service`
3. Add replace directive in `go.mod`:
   ```
   require github.com/openbear-it/health-esb v0.0.0
   replace github.com/openbear-it/health-esb => ../../
   ```
4. Write your handler (copy any existing service as template)
5. Add a `Dockerfile` in `deployments/docker/`
6. Add a K8s manifest in `deployments/k8s/`
7. Add the service to the matrix in `.github/workflows/build-images.yml`

Broker connection, retry, DLQ, correlation ID, and metrics are all provided by `internal/messaging`.


