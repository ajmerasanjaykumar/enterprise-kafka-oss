#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Step 7: Deploying Karapace (Open-Source Schema Registry)"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Applying Karapace Schema Registry Topic, Deployment & Service..."
kubectl apply -f "$ROOT_DIR/k8s/06-karapace/deployment.yaml"

echo "Waiting for Karapace Schema Registry Pod to be Ready..."
kubectl rollout status deployment/karapace-schema-registry -n kafka-enterprise --timeout=180s

echo "Updating Kafbat UI with Schema Registry endpoint..."
kubectl apply -f "$ROOT_DIR/k8s/04-kafbat-ui/configmap.yaml"
kubectl apply -f "$ROOT_DIR/k8s/04-kafbat-ui/deployment.yaml"
kubectl rollout restart deployment/kafbat-ui -n kafka-enterprise
kubectl rollout status deployment/kafbat-ui -n kafka-enterprise --timeout=180s

echo "========================================================"
echo "🎉 Karapace Schema Registry is active and wired to Kafbat UI!"
echo "Registry Internal URL: http://karapace-schema-registry.kafka-enterprise.svc.cluster.local:8081"
echo "Kafbat UI URL        : http://localhost:8080"
echo "Navigate to 'Schema Registry' in Kafbat UI to view schemas!"
echo "========================================================"
