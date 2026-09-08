#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Step 1: Setting up Kind Kubernetes Cluster"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

if kind get clusters 2>/dev/null | grep -q "enterprise-kafka"; then
    echo "✓ Kind cluster 'enterprise-kafka' is already running."
else
    echo "Creating kind cluster from $ROOT_DIR/kind/kind-config.yaml..."
    kind create cluster --config "$ROOT_DIR/kind/kind-config.yaml"
fi

kubectl cluster-info --context kind-enterprise-kafka

echo "Creating namespace 'kafka-enterprise'..."
kubectl apply -f "$ROOT_DIR/k8s/00-namespace/namespace.yaml"

echo "✓ Kubernetes cluster setup complete!"
