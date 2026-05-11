#!/usr/bin/env bash
# deploy-all.sh — Apply all Kubernetes manifests to the health-esb namespace

set -euo pipefail

KUBE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "→ Applying namespace"
kubectl apply -f "${KUBE_DIR}/namespace.yaml"

echo "→ Applying NATS"
kubectl apply -f "${KUBE_DIR}/nats/"

echo "→ Waiting for NATS to be ready"
kubectl wait --namespace=health-esb --for=condition=ready pod -l app=nats --timeout=60s

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
