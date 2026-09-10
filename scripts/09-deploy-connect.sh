#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Deploying Banking PostgreSQL & Kafka Connect"
echo "=========================================="

echo "1. Applying Banking PostgreSQL deployment..."
kubectl apply -f k8s/07-connect/postgres-db.yaml

echo "2. Applying Kafka Connect deployment & storage topics..."
kubectl apply -f k8s/07-connect/kafka-connect.yaml

echo "3. Updating Kafbat UI with Kafka Connect integration..."
kubectl apply -f k8s/04-kafbat-ui/configmap.yaml
kubectl apply -f k8s/04-kafbat-ui/deployment.yaml

echo "4. Waiting for PostgreSQL to be Ready..."
kubectl rollout status deployment/banking-postgres -n kafka-enterprise --timeout=120s

echo "5. Initializing Banking Ledger table in PostgreSQL..."
kubectl exec -n kafka-enterprise deployment/banking-postgres -- psql -U bankadmin -d banking -c "
CREATE TABLE IF NOT EXISTS bank_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL,
    amount NUMERIC(15,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE bank_transactions REPLICA IDENTITY FULL;
"

echo "6. Waiting for Kafka Connect to be Ready..."
kubectl rollout status deployment/kafka-connect -n kafka-enterprise --timeout=180s

echo "7. Restarting Kafbat UI to activate the Kafka Connect tab..."
kubectl rollout restart deployment/kafbat-ui -n kafka-enterprise
kubectl rollout status deployment/kafbat-ui -n kafka-enterprise --timeout=120s

echo "8. Registering Sample Connectors..."
sleep 5

# Register Debezium CDC Connector
kubectl exec -n kafka-enterprise deployment/kafka-connect -- curl -s -X POST \
  -H "Content-Type: application/json" \
  --data @- http://localhost:8083/connectors < k8s/07-connect/connectors/postgres-cdc-source.json || true

# Register Audit Sink Connector
kubectl exec -n kafka-enterprise deployment/kafka-connect -- curl -s -X POST \
  -H "Content-Type: application/json" \
  --data @- http://localhost:8083/connectors < k8s/07-connect/connectors/audit-file-sink.json || true

echo ""
echo "9. Active Connectors in Kafka Connect:"
kubectl exec -n kafka-enterprise deployment/kafka-connect -- curl -s http://localhost:8083/connectors

echo ""
echo "✓ Kafka Connect and 100% OSS Connectors successfully deployed and active in Kafbat UI!"
