#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "🧹 Deleting kind cluster enterprise-kafka"
echo "=========================================="

kind delete cluster --name enterprise-kafka

echo "✓ Cleanup complete!"
