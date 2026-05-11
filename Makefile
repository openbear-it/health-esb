.PHONY: run test lint docker k8s-deploy k8s-delete simulator build tidy

SERVICES := gateway adt-service lab-service fhir-bridge audit-service notification-service simulator

# ─── Development ─────────────────────────────────────────────────────────────

## run: Start NATS + all services via docker-compose
run:
	docker compose up --build

## run-nats: Start only NATS locally (for development without Docker)
run-nats:
	docker run --rm -d --name nats-dev -p 4222:4222 -p 8222:8222 nats:2.10-alpine -js -m 8222
	@echo "NATS started on localhost:4222 (monitoring: http://localhost:8222)"

## stop-nats: Stop the local NATS container
stop-nats:
	docker stop nats-dev || true

## dev: Run all services locally with hot-reload (requires air)
dev:
	@echo "Starting local services (requires NATS on localhost:4222)"
	@for svc in $(SERVICES); do \
		(cd apps/$$svc && go run . & echo "Started $$svc"); \
	done
	@echo "Dashboard: cd apps/dashboard && npm run dev"

# ─── Build ────────────────────────────────────────────────────────────────────

## build: Build all Go services
build:
	@for svc in $(SERVICES); do \
		echo "Building $$svc…"; \
		(cd apps/$$svc && go build ./...) || exit 1; \
	done
	@echo "All services built ✓"

## tidy: Run go mod tidy for all modules
tidy:
	go mod tidy
	@for svc in $(SERVICES); do \
		(cd apps/$$svc && go mod tidy); \
	done

# ─── Test ─────────────────────────────────────────────────────────────────────

## test: Run all Go tests
test:
	go test ./...
	@for svc in $(SERVICES); do \
		(cd apps/$$svc && go test ./...); \
	done

# ─── Lint ─────────────────────────────────────────────────────────────────────

## lint: Run golangci-lint
lint:
	golangci-lint run ./...
	@for svc in $(SERVICES); do \
		(cd apps/$$svc && golangci-lint run ./...); \
	done

# ─── Docker ──────────────────────────────────────────────────────────────────

## docker: Build all Docker images
docker:
	@for svc in $(SERVICES); do \
		echo "Building Docker image for $$svc…"; \
		docker build -f deployments/docker/Dockerfile.$$svc -t health-esb/$$svc:latest .; \
	done
	docker build -f deployments/docker/Dockerfile.dashboard -t health-esb/dashboard:latest .

## docker-push: Push Docker images to a registry (set REGISTRY env var)
docker-push: docker
	@for svc in $(SERVICES) dashboard; do \
		docker tag health-esb/$$svc:latest $(REGISTRY)/health-esb/$$svc:latest; \
		docker push $(REGISTRY)/health-esb/$$svc:latest; \
	done

# ─── Kubernetes ──────────────────────────────────────────────────────────────

## k8s-deploy: Apply all Kubernetes manifests
k8s-deploy:
	bash deployments/k8s/deploy-all.sh

## k8s-delete: Delete all Kubernetes resources in the health-esb namespace
k8s-delete:
	kubectl delete namespace health-esb

## k8s-status: Show pod status in health-esb namespace
k8s-status:
	kubectl get pods,svc,hpa -n health-esb

## k8s-logs: Tail logs from a specific service (SERVICE=gateway)
k8s-logs:
	kubectl logs -n health-esb -l app=$(SERVICE) -f --tail=100

# ─── Simulator ───────────────────────────────────────────────────────────────

## simulator: Run the traffic simulator locally
simulator:
	cd apps/simulator && go run .

# ─── Demo helpers ─────────────────────────────────────────────────────────────

## demo-admit: Send a single admission event via the gateway
demo-admit:
	curl -s -X POST http://localhost:8080/admissions \
		-H "Content-Type: application/json" \
		-d '{"patient_id":"P001","first_name":"John","last_name":"Doe","date_of_birth":"1980-03-15","ward":"ICU"}' | jq .

## demo-scale: Scale lab-service to 5 replicas to demonstrate horizontal scaling
demo-scale:
	kubectl scale deployment lab-service --replicas=5 -n health-esb
	@echo "lab-service scaled to 5 replicas"

## demo-chaos: Delete a lab-service pod to trigger restart/recovery demo
demo-chaos:
	kubectl delete pod -n health-esb -l app=lab-service --wait=false
	@echo "lab-service pod deleted — watch recovery at: make k8s-status"

# ─── Help ─────────────────────────────────────────────────────────────────────

## help: Show this help
help:
	@grep -E '^## ' Makefile | sed 's/^## //' | column -t -s ':'
