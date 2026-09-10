#!/usr/bin/env bash
set -euo pipefail

echo "========================================================="
echo "🚀 Deploying Full Open-Source Enterprise Kafka Platform"
echo "========================================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/01-setup-cluster.sh"
"$SCRIPT_DIR/02-deploy-strimzi.sh"
"$SCRIPT_DIR/03-deploy-opa.sh"
"$SCRIPT_DIR/04-deploy-kafka.sh"
"$SCRIPT_DIR/05-deploy-kafbat-ui.sh"
"$SCRIPT_DIR/06-test-rbac.sh"
"$SCRIPT_DIR/07-deploy-karapace.sh"
"$SCRIPT_DIR/08-test-schema-registry.sh"

echo "========================================================="
echo "🎉 DEPLOYMENT COMPLETE!"
echo "Kafka UI: http://localhost:8080"
echo "Schema Registry: http://karapace-schema-registry.kafka-enterprise.svc.cluster.local:8081"
echo "========================================================="

