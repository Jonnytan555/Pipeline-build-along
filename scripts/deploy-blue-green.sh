#!/usr/bin/env bash
# Blue-green deploy via Argo Rollouts.
# The old (blue) version stays live until the new (green) passes analysis, then switches instantly.
# Usage: ./scripts/deploy-blue-green.sh <image-tag>
set -euo pipefail

TAG="${1:?Usage: deploy-blue-green.sh <image-tag>}"

echo "==> Blue-green deploy: tag=${TAG}"

# Update the Rollout image — Argo Rollouts handles the rest
kubectl argo rollouts set image pipeline-api-bg \
  api="ghcr.io/jonnytan555/e2e-pipeline:${TAG}" \
  -n pipeline

echo "==> Watching rollout (green is live alongside blue)..."
kubectl argo rollouts get rollout pipeline-api-bg -n pipeline --watch &
WATCH_PID=$!

echo ""
echo "  Green (preview) is running. Run smoke tests, then promote:"
echo "  kubectl argo rollouts promote pipeline-api-bg -n pipeline"
echo ""
echo "  To abort and stay on blue:"
echo "  kubectl argo rollouts abort pipeline-api-bg -n pipeline"
echo ""

wait $WATCH_PID
