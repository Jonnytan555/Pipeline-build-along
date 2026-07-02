# Stage 3 — Kubernetes + Helm

Deploys the pipeline API service to Kubernetes using Helm. Covers rolling updates, blue-green, and canary deployment strategies via Argo Rollouts.

## What it does

- Scaffolds Kubernetes manifests for the FastAPI service (Deployment, Service, Ingress, ConfigMap, Secret)
- Provides a Helm chart with environment-specific value overrides (AWS, Azure, GCP, on-prem)
- Implements progressive delivery via Argo Rollouts (canary traffic shifting + Prometheus-based auto-promotion)

## Files

| File | Purpose |
|---|---|
| `k8s_walkthrough.py` | Concept walkthrough — reads manifests, checks cluster status, helm dry-run |
| *(manifests live in `kubernetes/` and `helm/` at project root)* | |

See [kubernetes/](../../kubernetes/) and [helm/](../../helm/) for the live manifests.

## Run walkthrough

```bash
python -m stages.stage-3-kubernetes-helm.k8s_walkthrough
```

## Key manifests

| File | Purpose |
|---|---|
| [kubernetes/deployment.yaml](../../kubernetes/deployment.yaml) | 2-replica rolling update with readiness/liveness probes |
| [kubernetes/rollout-canary.yaml](../../kubernetes/rollout-canary.yaml) | Canary: 5% → 20% → 50% → 100% with Prometheus analysis |
| [kubernetes/rollout-blue-green.yaml](../../kubernetes/rollout-blue-green.yaml) | Blue-green with manual promotion gate |
| [kubernetes/analysis-templates.yaml](../../kubernetes/analysis-templates.yaml) | Auto-promote if success_rate ≥ 95% AND error_rate < 1% |
| [helm/e2e-pipeline/](../../helm/e2e-pipeline/) | Parameterised chart with 4 environment value files |

## Deployment strategies

| Strategy | Script | When to use |
|---|---|---|
| Rolling update | `scripts/deploy.sh <tag>` | Default — zero downtime, low cost |
| Blue-green | `scripts/deploy-blue-green.sh <tag>` | Instant cutover, smoke test before switch |
| Canary | `scripts/deploy-canary.sh <tag>` | Riskiest changes — gradual traffic shift |

## Helm commands

```bash
# First deploy (AWS)
helm install e2e-pipeline helm/e2e-pipeline -f helm/e2e-pipeline/values-aws.yaml

# Update image tag
helm upgrade e2e-pipeline helm/e2e-pipeline \
  -f helm/e2e-pipeline/values-aws.yaml \
  --set image.tag=v2.1.0

# Rollback to previous release
helm rollback e2e-pipeline 1
```

## Connect kubectl to EKS (once cluster is running)

```bash
aws eks update-kubeconfig --region eu-west-2 --name pipeline-cluster
kubectl get nodes
```

## Pipeline position

```
Stage 2 (Terraform creates VPC + EKS cluster)
      ↓
[Stage 3: Kubernetes deploys pipeline services into that cluster]
      ↓
Stage 4 (Great Expectations validates data quality)
```
