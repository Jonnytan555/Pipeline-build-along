#!/usr/bin/env bash
# Canary deploy via Argo Rollouts.
# Routes 5% → 20% → 50% → 100% of traffic; auto-promotes if Prometheus metrics pass.
# Usage: ./scripts/deploy-canary.sh <image-tag>
set -euo pipefail

TAG="${1:?Usage: deploy-canary.sh <image-tag>}"

echo "==> Canary deploy: tag=${TAG}"
echo "    Traffic flow: 5% → 20% → 50% → 100%"
echo "    Auto-abort if: success_rate < 95% OR error_rate >= 1%"
echo ""

kubectl argo rollouts set image pipeline-api \
  api="ghcr.io/jonnytan555/e2e-pipeline:${TAG}" \
  -n pipeline

echo "==> Watching canary rollout..."
kubectl argo rollouts get rollout pipeline-api -n pipeline --watch

echo ""
echo "To manually promote past a pause step:"
echo "  kubectl argo rollouts promote pipeline-api -n pipeline"
echo ""
echo "To abort and roll back immediately:"
echo "  kubectl argo rollouts abort pipeline-api -n pipeline"
