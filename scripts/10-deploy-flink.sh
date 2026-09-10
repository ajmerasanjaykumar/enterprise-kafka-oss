#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Deploying Apache Flink & Streaming Engine"
echo "=========================================="

echo "1. Applying Apache Flink Session Cluster (JobManager + TaskManager)..."
kubectl apply -f k8s/08-flink/flink-cluster.yaml

echo "2. Applying Streaming Fraud Detection Pipeline..."
kubectl apply -f k8s/08-flink/streaming-fraud-detector.yaml

echo "3. Waiting for Flink JobManager to be Ready..."
kubectl rollout status deployment/flink-jobmanager -n kafka-enterprise --timeout=180s

echo "4. Waiting for Flink TaskManager to be Ready..."
kubectl rollout status deployment/flink-taskmanager -n kafka-enterprise --timeout=180s

echo "5. Waiting for Streaming Fraud Detector to be Ready..."
kubectl rollout status deployment/streaming-fraud-detector -n kafka-enterprise --timeout=180s

echo "6. Checking Flink Web Dashboard status..."
kubectl exec -n kafka-enterprise deployment/flink-jobmanager -- curl -s http://localhost:8081/overview || true

echo ""
echo "✓ Apache Flink cluster and Real-Time Streaming Fraud Engine successfully deployed!"
echo "  To access Flink Web UI Dashboard locally: kubectl port-forward svc/flink-jobmanager 8082:8081 -n kafka-enterprise"
