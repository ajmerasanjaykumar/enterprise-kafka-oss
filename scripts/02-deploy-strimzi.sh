#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Step 2: Deploying Strimzi Kafka Operator"
echo "=========================================="

echo "Adding Strimzi Helm repository..."
helm repo add strimzi https://strimzi.io/charts/ || true
helm repo update

echo "Installing Strimzi Kafka Operator in namespace 'kafka-enterprise'..."
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    --namespace kafka-enterprise

echo "Waiting for Strimzi Operator Pod to be Ready..."
kubectl rollout status deployment/strimzi-cluster-operator -n kafka-enterprise --timeout=180s

echo "✓ Strimzi Kafka Operator successfully deployed!"
