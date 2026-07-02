"""
Stage 3 — Kubernetes + Helm
============================
CONCEPT: Kubernetes (K8s) is a container orchestrator. You describe the desired
state (N replicas of this container, this much CPU/memory, this port exposed),
and K8s continuously reconciles actual state toward desired state.

KUBERNETES OBJECTS USED IN THIS PROJECT (kubernetes/ + helm/)
──────────────────────────────────────────────────────────────
  Deployment      — manages a set of identical Pods; handles rollouts and restarts
  Service         — stable DNS name + IP for a set of Pods (ClusterIP / LoadBalancer)
  Ingress         — routes external HTTP(S) traffic to Services by path/host
  ConfigMap       — non-secret config (env vars, config files) injected into Pods
  Secret          — base64-encoded sensitive config (passwords, API keys)
  Namespace       — logical isolation within a cluster (like separate tenants)
  ServiceMonitor  — tells Prometheus which Pods to scrape for metrics (Operator CRD)

THE DEPLOYMENT STRATEGIES IN THIS PROJECT (scripts/)
──────────────────────────────────────────────────────
  deploy.sh            — standard rolling update
  deploy-blue-green.sh — keep old version running until new is verified, then switch
  deploy-canary.sh     — route 5% of traffic to new version, watch metrics, then promote

HELM — KUBERNETES PACKAGE MANAGER
───────────────────────────────────
  Helm converts parameterised templates into Kubernetes YAML.
  Instead of editing 8 YAML files every time you deploy to a new environment,
  you have one values.yaml per environment.

  helm/e2e-pipeline/
    Chart.yaml        — chart metadata (name, version, description)
    values.yaml       — default values
    values-aws.yaml   — AWS-specific overrides
    values-azure.yaml — Azure-specific overrides
    values-gcp.yaml   — GCP-specific overrides
    values-onprem.yaml— On-premises overrides
    templates/        — YAML templates with {{ .Values.xxx }} placeholders

  helm install e2e-pipeline ./helm/e2e-pipeline -f values-aws.yaml
  helm upgrade  e2e-pipeline ./helm/e2e-pipeline -f values-aws.yaml --set image.tag=v2.1.0
  helm rollback e2e-pipeline 1   ← instantly roll back to previous release

ARGO ROLLOUTS (kubernetes/rollout-canary.yaml, rollout-blue-green.yaml)
─────────────────────────────────────────────────────────────────────────
  Argo Rollouts extends K8s with progressive delivery strategies.
  Instead of a Deployment, you use a Rollout object:
    - canary: gradually shift traffic (5% → 20% → 50% → 100%), auto-pause if metrics degrade
    - blue-green: spin up new version, run smoke tests, then switch instantly

  The AnalysisTemplate (kubernetes/analysis-templates.yaml) defines success criteria:
    - success rate > 95% in Prometheus
    - error rate < 1%
  Argo Rollouts auto-promotes if metrics pass, auto-aborts if they fail.

Run:
  # Prerequisite: kubectl configured to point at your EKS cluster (Stage 2)
  # Get credentials: aws eks update-kubeconfig --region eu-west-1 --name pipeline-cluster

  python stages/stage-3-kubernetes-helm/k8s_walkthrough.py
"""

import subprocess
import sys
from pathlib import Path

ROOT       = Path(__file__).parent.parent.parent
K8S_DIR    = ROOT / "kubernetes"
HELM_DIR   = ROOT / "helm" / "e2e-pipeline"
SCRIPTS_DIR = ROOT / "scripts"


def kubectl(args: list[str]) -> tuple[int, str]:
    try:
        result = subprocess.run(
            ["kubectl"] + args, capture_output=True, text=True,
        )
        return result.returncode, result.stdout + result.stderr
    except FileNotFoundError:
        return 1, "kubectl not found"


def helm(args: list[str]) -> tuple[int, str]:
    try:
        result = subprocess.run(
            ["helm"] + args, capture_output=True, text=True,
        )
        return result.returncode, result.stdout + result.stderr
    except FileNotFoundError:
        return 1, "helm not found"


def kubectl_available() -> bool:
    code, _ = kubectl(["version", "--client"])
    return code == 0


def helm_available() -> bool:
    code, _ = helm(["version"])
    return code == 0


def explain_deployment_yaml() -> None:
    """
    A Deployment manages a ReplicaSet which manages Pods.

    Rolling update strategy (default):
      maxUnavailable: 1  → at most 1 Pod down during update (no full outage)
      maxSurge: 1        → at most 1 extra Pod during update (controls cost)

    Key fields in kubernetes/deployment.yaml:
      - image: ghcr.io/org/e2e-pipeline:v1.0.0  → pin to a specific tag, never :latest
      - resources.requests   → K8s uses this to schedule the Pod on a node
      - resources.limits     → K8s kills the Pod if it exceeds this
      - readinessProbe       → Pod not added to Service until this passes
      - livenessProbe        → Pod is restarted if this fails
    """
    path = K8S_DIR / "deployment.yaml"
    if path.exists():
        print("\n── kubernetes/deployment.yaml ────────────────────────────────")
        print(path.read_text()[:2000])


def explain_helm_values() -> None:
    """
    values.yaml is the single source of truth for what your chart deploys.
    You override it per environment without touching templates.

    Pattern: helm upgrade --install is idempotent (install if not exists, upgrade if it does).
    Use it in CI/CD so the same command works for first deploy and every update.
    """
    path = HELM_DIR / "values.yaml"
    if path.exists():
        print("\n── helm/e2e-pipeline/values.yaml ─────────────────────────────")
        print(path.read_text()[:2000])


def explain_canary_rollout() -> None:
    """
    Canary releases reduce deployment risk by routing a small fraction of traffic
    to the new version first. If something goes wrong, only that fraction is affected.

    kubernetes/rollout-canary.yaml uses Argo Rollouts with this strategy:
      - 5%  of traffic → new version (step 1: pause and observe)
      - Run AnalysisRun against Prometheus metrics
      - If error rate stays < 1% for 2 minutes → promote to 20%
      - 20% → 50% → 100% with analysis at each step
      - If any step fails analysis → automatic rollback to previous version

    Compare to a rolling update:
      Rolling: 100% of traffic hits new version before you know if it works
      Canary:  5% of traffic hits new version; you know before it's too late
    """
    path = K8S_DIR / "rollout-canary.yaml"
    if path.exists():
        print("\n── kubernetes/rollout-canary.yaml ────────────────────────────")
        print(path.read_text()[:2000])


def live_cluster_status() -> None:
    if not kubectl_available():
        print("\n  kubectl not found. Install: https://kubernetes.io/docs/tasks/tools/")
        return

    print("\n── Live cluster status ────────────────────────────────────────")
    code, out = kubectl(["cluster-info"])
    if code != 0:
        print("  kubectl not connected to a cluster.")
        print("  After Stage 2 (Terraform): aws eks update-kubeconfig --region eu-west-1 --name pipeline-cluster")
        return

    print(out)
    _, out = kubectl(["get", "pods", "--all-namespaces", "--no-headers"])
    print(out[:2000])


def helm_dry_run() -> None:
    if not helm_available():
        print("\n  helm not found. Install: https://helm.sh/docs/intro/install/")
        return

    print("\n── helm template dry-run (renders manifests, no cluster needed) ──")
    code, out = helm([
        "template", "e2e-pipeline", str(HELM_DIR),
        "-f", str(HELM_DIR / "values.yaml"),
        "--debug",
    ])
    if code == 0:
        print(out[:3000], "\n  ... (truncated)")
    else:
        print("  helm template failed:")
        print(out[:1000])


def run_walkthrough() -> None:
    print("\n── Stage 3: Kubernetes + Helm Walkthrough ──")
    print("""
CONCEPT RECAP
─────────────
  Kubernetes solves: "How do I run N copies of my container reliably and scale them?"
  Helm solves: "How do I deploy the same chart to dev/staging/prod with different configs?"
  Argo Rollouts solves: "How do I deploy without risking a full outage?"

  After Stage 2 (Terraform) you have:
    - An EKS cluster running in AWS
    - kubectl configured to talk to it
    - Helm installed locally

  In this stage you deploy the pipeline services to that cluster.
""")

    explain_deployment_yaml()
    explain_helm_values()
    explain_canary_rollout()
    live_cluster_status()
    helm_dry_run()

    print("""
DEPLOY COMMANDS (once cluster is running)
──────────────────────────────────────────
  # First deploy
  helm install e2e-pipeline helm/e2e-pipeline -f helm/e2e-pipeline/values-aws.yaml

  # Update image tag (e.g. after a new CI build)
  helm upgrade e2e-pipeline helm/e2e-pipeline \\
    -f helm/e2e-pipeline/values-aws.yaml \\
    --set image.tag=v2.1.0

  # Rollback if something goes wrong
  helm rollback e2e-pipeline 1

  # Watch rollout progress (Argo Rollouts)
  kubectl argo rollouts get rollout e2e-pipeline --watch
""")


if __name__ == "__main__":
    run_walkthrough()
