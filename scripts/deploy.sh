#!/usr/bin/env bash
# Standard rolling-update deploy.
# Usage: ./scripts/deploy.sh <image-tag> [values-file]
# Example: ./scripts/deploy.sh v2.1.0 values-aws.yaml
set -euo pipefail

TAG="${1:?Usage: deploy.sh <image-tag> [values-file]}"
VALUES="${2:-helm/e2e-pipeline/values-aws.yaml}"

echo "==> Rolling deploy: tag=${TAG}, values=${VALUES}"

helm upgrade --install e2e-pipeline helm/e2e-pipeline \
  -f "${VALUES}" \
  --set image.tag="${TAG}" \
  --namespace pipeline \
  --create-namespace \
  --wait \
  --timeout 5m

echo "==> Watching rollout..."
kubectl rollout status deployment/e2e-pipeline -n pipeline --timeout=3m

echo "==> Deploy complete."
kubectl get pods -n pipeline -l app.kubernetes.io/name=e2e-pipeline
