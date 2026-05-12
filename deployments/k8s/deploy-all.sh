#!/usr/bin/env bash
# deploy-all.sh — Apply all Kubernetes manifests to the health-esb namespace

set -euo pipefail

KUBE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "→ Applying namespace"
kubectl apply -f "${KUBE_DIR}/namespace.yaml"

echo "→ Applying local-path StorageClass provisioner"
kubectl apply -f "${KUBE_DIR}/rabbitmq/rabbitmq-local-path-storage.yaml"

echo "→ Applying RabbitMQ Cluster Operator"
kubectl apply -f "${KUBE_DIR}/rabbitmq/rabbitmq-cluster-operator.yaml"

echo "→ Waiting for RabbitMQ Operator to be ready"
kubectl wait --namespace=rabbitmq-system \
  --for=condition=available deployment/rabbitmq-cluster-operator \
  --timeout=120s

echo "→ Applying RabbitMQ cluster"
kubectl apply -f "${KUBE_DIR}/rabbitmq/rabbitmq-custom.yaml"

echo "→ Waiting for RabbitMQ to be ready"
kubectl wait --namespace=rabbitmq \
  --for=condition=ready pod -l app=RabbitmqCluster \
  --timeout=180s

echo "→ Applying rabbitmq-secret to health-esb namespace"
kubectl apply -f "${KUBE_DIR}/rabbitmq/rabbitmq-secret.yaml"

echo "→ Applying services"
for svc in gateway adt-service lab-service fhir-bridge audit-service notification-service simulator dashboard; do
  kubectl apply -f "${KUBE_DIR}/${svc}/"
done

echo "→ Applying monitoring"
kubectl apply -f "${KUBE_DIR}/monitoring/"

echo ""
echo "✓ All manifests applied to namespace health-esb"
echo ""
echo "Pods:"
kubectl get pods -n health-esb
