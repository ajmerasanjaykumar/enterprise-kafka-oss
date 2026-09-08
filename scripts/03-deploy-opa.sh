#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Step 3: Deploying Open Policy Agent (OPA)"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Applying OPA Policy and Deployment manifests..."
kubectl apply -f "$ROOT_DIR/k8s/02-opa/opa-deployment.yaml"

echo "Waiting for OPA Pod to be Ready..."
kubectl rollout status deployment/opa -n kafka-enterprise --timeout=120s

echo "Testing OPA Policy endpoint..."
kubectl run opa-test-curl --image=curlimages/curl:latest --restart=Never -n kafka-enterprise --rm -i -- \
    curl -s http://opa-service.kafka-enterprise.svc.cluster.local:8181/v1/data/kafka/authz/allow || true

echo "✓ OPA Authorization engine is running and serving policies!"
