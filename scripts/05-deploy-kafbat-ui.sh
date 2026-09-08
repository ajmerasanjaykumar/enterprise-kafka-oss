#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Step 5: Deploying Kafbat UI (Control Center Alternative)"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Applying Kafbat UI ConfigMap, Deployment & Service..."
kubectl apply -f "$ROOT_DIR/k8s/04-kafbat-ui/configmap.yaml"
kubectl apply -f "$ROOT_DIR/k8s/04-kafbat-ui/deployment.yaml"

echo "Waiting for Kafbat UI Pod to be Ready..."
kubectl rollout status deployment/kafbat-ui -n kafka-enterprise --timeout=180s

echo "========================================================"
echo "🎉 Kafbat UI is running!"
echo "Access URL: http://localhost:8080"
echo ""
echo "Demonstration Logins (RBAC Demo):"
echo "  1. Admin (SystemAdmin)         : admin     / admin123"
echo "  2. Team A Manager (ResourceOwner): a-manager / manager123"
echo "  3. Team A Writer (DeveloperWrite): a-writer  / writer123"
echo "  4. Team A Reader (DeveloperRead) : a-reader  / reader123"
echo "  5. Team B Manager (ResourceOwner): b-manager / manager123"
echo "  6. Team B Writer (DeveloperWrite): b-writer  / writer123"
echo "  7. Team B Reader (DeveloperRead) : b-reader  / reader123"
echo "========================================================"
