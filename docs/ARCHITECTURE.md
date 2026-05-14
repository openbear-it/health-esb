# Health ESB — Architecture & Quick Start

A production-ready demo of a **healthcare integration backbone** built with:

- **Go 1.25**
- **[Watermill](https://watermill.io)** — event routing & middleware
- **RabbitMQ (AMQP)** — durable fanout exchanges + per-service queues
- **Kubernetes / K3s** — cloud-native deployment
- **FHIR R4** — healthcare interoperability
- **Prometheus + Grafana + OpenTelemetry** — full observability stack

> The goal is to demonstrate how easy it is to build resilient, observable healthcare integration systems with Watermill.

---

## Architecture

```
 ╔══════════════════════════════════════════════════════════════════╗
 ║                    RabbitMQ (AMQP 0-9-1)                        ║
 ║         durable fanout exchanges · per-service queues           ║
 ╚══════════════════════════════════════════════════════════════════╝
         ▲  │              │              │              │
   REST  │  │              │              │              │
   POST  │  ▼              ▼              ▼              ▼
  ┌──────┴──────┐  ┌─────────────┐  ┌──────────┐  ┌─────────────┐
  │   gateway   │  │ adt-service │  │lab-service│  │ fhir-bridge │
  │  REST + SSE │  │             │  │           │  │             │
  │  + sim-ctl  │  └─────────────┘  └──────────┘  └─────────────┘
  │  + chaos    │
  └─────────────┘       ┌──────────────────────────────────────┐
         │ SSE          │         notification-service         │
         │              └──────────────────────────────────────┘
         │              ┌──────────────────────────────────────┐
         │              │  audit-service  (all topics)         │
         │              │  → audit.jsonl + GET /audit          │
         │              │  + POST /dlq/requeue                 │
         │              └──────────────────────────────────────┘
         ▼
  ┌─────────────┐
  │  dashboard  │  React — SSE stream + control panel
  └─────────────┘
```

---

## Event Flow

```
① Client calls POST /admissions  (or built-in simulator fires)
        │
        ▼
② gateway publishes  command-patient-admit
        │
        ├──► adt-service  →  patient-admitted
        │         │
        │         ├──► lab-service  →  lab-result-created  (×5 tests)
        │         │         ├──► fhir-bridge  →  fhir-document-created
        │         │         └──► notification-service  (SMS if abnormal)
        │         │
        │         └──► notification-service  (admission email)
        │
        └──► audit-service  (every topic → audit.jsonl)

③ POST /discharges   →  patient-discharged
④ POST /transfers    →  patient-transferred
⑤ POST /alerts       →  alert-created

⑥ SSE broker at gateway forwards ALL events to the React dashboard.
```

---

## Internal packages

```
internal/
├── events/           MessageEnvelope, Event type, topic constants, payloads
├── messaging/        Router factory · middleware chain · RouterBuilder
├── config/           Env config · YAML route config · fsnotify hot-reload
├── observability/    slog logger · Prometheus metrics · OTel tracing
├── resilience/       Circuit breaker (Closed/Open/HalfOpen)
├── transformer/      Transformer interface · Registry · FhirObservationTransformer
├── fhir/             FHIR R4 Observation builder
└── hl7/              Minimal HL7 v2 segment parser
```

---

## MessageEnvelope

Every message published to RabbitMQ is wrapped in a `MessageEnvelope`:

```go
type MessageEnvelope struct {
    ID            string            // UUID v4, generated at publish time
    CorrelationID string            // propagated across the whole chain
    CausationID   string            // ID of the message that caused this one
    Version       string            // "1.0" for schema evolution
    Source        string            // SERVICE_NAME env var
    Type          string            // e.g. "lab-result-created"
    Timestamp     time.Time
    Headers       map[string]string // W3C traceparent injected here
    Payload       json.RawMessage
}
```

Use `events.MarshalEnvelope(...)` to publish and `events.UnmarshalEnvelope(data)` to consume.

---

## Middleware chain

Every Watermill router is preconfigured with this exact order:

```
CorrelationID → Recoverer → Idempotency → Retry → PoisonQueue
```

| Middleware | Purpose |
|---|---|
| `CorrelationID` | Ensures the same `correlationID` flows through the whole chain |
| `Recoverer` | Catches panics, nacks so the message is retried |
| `Idempotency` | Deduplicates by envelope ID (in-memory, 10 min TTL, pluggable store) |
| `Retry` | 5 attempts, exponential backoff: 1 s → 2 s → 4 s → 8 s → 16 s |
| `PoisonQueue` | After 5 failures routes to `<topic>-dlq` (registered **per handler**) |

The idempotency store is pluggable via `IdempotencyStore`:

```go
type IdempotencyStore interface {
    Has(id string) bool
    Add(id string, ttl time.Duration)
}
```

---

## OTel trace propagation

`messaging.TracingMiddleware(serviceName, tp)` propagates W3C `traceparent` through `MessageEnvelope.Headers`.

- **Publish side:** call `messaging.InjectSpanIntoEnvelope(ctx, env)` before marshalling.
- **Consume side:** the middleware extracts the parent span, starts a child span named `<service>/<topic>/handle`, and ends it with OK or ERROR status.

---

## Route configuration (YAML)

Routes are loaded from `config/routes.yaml` (path overridable with `ROUTES_CONFIG_PATH`):

```yaml
routes:
  - name: adt-process-admit
    input_topic: command-patient-admit
    output_topic: patient-admitted
    handler: adt_handle_admit
    enabled: true
    retry_policy:
      max_retries: 5
      initial_interval: 1s
      multiplier: 2.0
```

`config.WatchRoutes(ctx, path, logger)` debounces file-system events (200 ms) and emits a new `[]RouteConfig` on a channel. `messaging.RouterBuilder` consumes the channel and hot-reloads the router with zero downtime, logging added/removed/changed routes.

---

## Transformer registry

```go
type Transformer interface {
    Name()      string
    Transform(payload json.RawMessage) (json.RawMessage, error)
}
```

Built-in transformers:

| Name | Description |
|---|---|
| `""` | `PassthroughTransformer` — returns payload unchanged (default) |
| `"hl7_to_fhir"` | `FhirObservationTransformer` — `LabResultPayload` → FHIR R4 Observation |

Register additional transformers at startup with `registry.Register(t)`.

---

## Circuit breaker

`resilience.NewBreaker(cfg)` wraps any `func() error` call:

| State | Description |
|---|---|
| Closed | Normal operation — failures are counted |
| Open | Calls rejected immediately with `ErrCircuitOpen` |
| HalfOpen | One probe allowed after `ResetTimeout` (default 30 s) |

**Thresholds:** 5 consecutive failures → Open. Probe success → Closed. Probe failure → Open again.

State is exported to the `healthesb_circuit_breaker_state` Prometheus gauge (`target`, `state` labels). Applied in `notification-service` around outbound email/SMS sends.

---

## Prometheus metrics

| Metric | Type | Labels |
|---|---|---|
| `healthesb_<svc>_messages_processed_total` | Counter | `topic` |
| `healthesb_<svc>_messages_failed_total` | Counter | `topic` |
| `healthesb_<svc>_message_processing_duration_seconds` | Histogram | `topic` |
| `healthesb_<svc>_retry_total` | Counter | `topic` |
| `healthesb_<svc>_dlq_total` | Counter | `topic` |
| `healthesb_message_e2e_duration_seconds` | Histogram | `source_service`, `dest_service`, `topic` |
| `healthesb_queue_depth_total` | Gauge | `queue_name` |
| `healthesb_circuit_breaker_state` | Gauge | `target`, `state` |

`metrics.StartQueueDepthPoller(ctx, mgmtURL, logger)` polls `/api/queues` every 15 s.

---

## Audit log

`audit-service` writes every received event to `$AUDIT_LOG_PATH` as NDJSON.

- **Daily rotation:** at midnight the current file is renamed to `audit-YYYY-MM-DD.jsonl`; files older than 30 days are deleted.
- **Query API:** `GET /audit?from=<RFC3339>&to=<RFC3339>&type=<event_type>` streams matching records.

---

## Dead-letter requeue API

```
POST /dlq/requeue   (audit-service · Basic Auth)
{ "topic": "lab-result-created-dlq", "limit": 10 }
```

Reads up to `limit` messages from the DLQ queue and republishes to the original topic.

---

## Quick Start

### With Docker Compose

```bash
make run            # start RabbitMQ + all services + dashboard + observability
open http://localhost:3000
make demo-admit
```

### Local development

```bash
make run-rabbitmq   # or: docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management

cd apps/gateway              && go run .
cd apps/adt-service          && go run .
cd apps/lab-service          && go run .
cd apps/fhir-bridge          && go run .
cd apps/audit-service        && go run .
cd apps/notification-service && go run .

curl -N http://localhost:8080/events/stream   # watch SSE stream
cd apps/dashboard && npm install && npm run dev   # open http://localhost:3000
```

---

## Kubernetes Deployment

```bash
make docker && make k8s-deploy && make k8s-status
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `SERVICE_NAME` | service-specific | Identifier in logs, metrics, envelopes |
| `PORT` | `8080` | HTTP listen port |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ endpoint |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `OTLP_ENDPOINT` | `http://localhost:4318` | OpenTelemetry collector (OTLP HTTP) |
| `ROUTES_CONFIG_PATH` | `config/routes.yaml` | YAML route configuration file |
| `AUDIT_LOG_PATH` | `/var/log/health-esb/audit.jsonl` | Audit log path (audit-service) |
| `DLQ_USER` | `admin` | Basic auth username for `/dlq/requeue` |
| `DLQ_PASSWORD` | `""` | Basic auth password for `/dlq/requeue` |

---

## Adding a new service

1. `go mod init github.com/openbear-it/health-esb/apps/my-service`
2. Add `use ./apps/my-service` to `go.work`
3. Copy the handler pattern from any existing service
4. Add the route to `config/routes.yaml`
5. Add `Dockerfile` in `deployments/docker/` and a K8s manifest in `deployments/k8s/`

Broker connection, middleware chain, envelope marshalling, metrics, and tracing are all provided by `internal/messaging` and `internal/observability`.
