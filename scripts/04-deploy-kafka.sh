#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Step 4: Deploying Kafka Cluster & Topics"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Applying Kafka Cluster Custom Resources..."
kubectl apply -f "$ROOT_DIR/k8s/03-kafka/kafka-cluster.yaml"

echo "Waiting for Kafka Dual-Role Cluster to be Ready..."
kubectl wait kafka/enterprise-kafka --for=condition=Ready --timeout=300s -n kafka-enterprise

echo "Applying Kafka Topics..."
kubectl apply -f "$ROOT_DIR/k8s/03-kafka/kafka-topics.yaml"

echo "Applying Kafka Users..."
kubectl apply -f "$ROOT_DIR/k8s/03-kafka/kafka-users.yaml"

echo "Listing deployed topics in Strimzi:"
kubectl get kafkatopics -n kafka-enterprise

echo "✓ Enterprise Kafka Cluster & Topics deployed!"
