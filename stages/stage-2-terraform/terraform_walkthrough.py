"""
Stage 2 — Terraform: Infrastructure as Code
=============================================
CONCEPT: Describe your infrastructure declaratively. Terraform figures out what
to create, change, or destroy to reach that state.

WHY IaC (Infrastructure as Code)?
──────────────────────────────────
Before IaC: "click ops" — clicking through the AWS console to create resources.
  Problems:
  - Not reproducible: can't recreate the same setup in a new account
  - Not reviewable: no PR, no history, no diff
  - Drift: what you clicked months ago doesn't match what runs today

With Terraform:
  - `terraform plan`  → shows exactly what will change, before changing it
  - `terraform apply` → makes only the changes shown in plan
  - `terraform destroy` → tears everything down cleanly
  - State file (.tfstate) → Terraform knows what already exists in AWS

THE TERRAFORM FILES IN THIS PROJECT (terraform/)
──────────────────────────────────────────────────
  providers.tf    — which cloud (AWS) and region; the Terraform version constraints
  variables.tf    — inputs (cluster name, region, instance size etc.)
  terraform.tfvars.example — example values; copy to terraform.tfvars, fill in
  networking.tf   — VPC, subnets, route tables, security groups
  eks.tf          — Kubernetes cluster (EKS) and node groups
  rds.tf          — PostgreSQL on RDS (the managed DB for Airflow + pipeline)
  s3.tf           — S3 bucket for artifacts, Spark checkpoints, MLflow models
  security.tf     — IAM roles, policies (EKS service accounts, RDS access)
  ec2.tf          — Bastion host for SSH access to private subnets
  load-balancer-controller.tf — AWS Load Balancer Controller (for K8s Ingress)
  outputs.tf      — values printed after apply (cluster endpoint, RDS hostname etc.)

TERRAFORM WORKFLOW
──────────────────
  terraform init    ← download providers (runs once per new workspace)
  terraform plan    ← diff desired vs actual state; shows what will change
  terraform apply   ← execute the plan; prompts for confirmation
  terraform destroy ← tear everything down

State management:
  Local state  (default) → .tfstate file on your machine; fine for learning
  Remote state (production) → stored in S3 + DynamoDB locking; prevents two people
                              applying at the same time

HOW STATE WORKS
───────────────
  1. You write .tf files describing what you want
  2. Terraform reads the state file to know what currently exists
  3. Terraform calls AWS APIs to find the actual state of resources
  4. Terraform computes the diff and shows you a plan
  5. On apply, Terraform updates the real resources AND the state file atomically

KEY PATTERN: MODULES
────────────────────
  A module is a reusable set of .tf files. This project uses the official modules:
    module "eks" {
      source  = "terraform-aws-modules/eks/aws"
      version = "~> 20.0"
      ...
    }
  The module encapsulates 50+ resources. You pass in variables; it outputs
  the cluster endpoint, certificate authority, etc. that other resources need.

Run:
  cd terraform/
  terraform init
  terraform plan -var-file=terraform.tfvars
  # Review the plan. When ready:
  terraform apply -var-file=terraform.tfvars
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
TERRAFORM_DIR = ROOT / "terraform"


def run(cmd: list[str], cwd: Path | None = None) -> tuple[int, str]:
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        cwd=str(cwd or TERRAFORM_DIR),
    )
    return result.returncode, result.stdout + result.stderr


def terraform_available() -> bool:
    code, _ = run(["terraform", "version"])
    return code == 0


def explain_providers() -> None:
    """
    providers.tf pins the AWS provider version and sets the region.
    This ensures every team member uses the same provider version.
    Pinning prevents unexpected breaking changes from provider upgrades.
    """
    path = TERRAFORM_DIR / "providers.tf"
    if path.exists():
        print("\n── providers.tf ──────────────────────────────────────────────")
        print(path.read_text())
    else:
        print("\n  terraform/providers.tf not found")


def explain_variables() -> None:
    """
    variables.tf declares inputs with types, descriptions, and defaults.
    You override defaults in terraform.tfvars (git-ignored) or via -var flags.

    Rule: sensitive values (passwords, tokens) should NEVER have defaults.
    Terraform will prompt interactively, or you set them via TF_VAR_* env vars:
      export TF_VAR_db_password=mysecretpassword
    """
    path = TERRAFORM_DIR / "variables.tf"
    if path.exists():
        print("\n── variables.tf ──────────────────────────────────────────────")
        print(path.read_text()[:2000], "...")


def show_plan_output() -> None:
    """
    `terraform plan` is your safety net. Read the plan carefully before applying.

    Legend:
      + create    → new resource will be created
      ~ update    → existing resource will be modified in-place
      - destroy   → resource will be deleted (CAREFUL — check this)
      -/+ replace → must destroy and recreate (e.g. changing RDS instance class)

    A replace (-/+) on an RDS instance or EKS node group means DOWNTIME.
    Always look for these before applying in production.
    """
    if not terraform_available():
        print("\n  Terraform not installed.")
        print("  Install: https://developer.hashicorp.com/terraform/downloads")
        print("\n  Without Terraform, study the concept:")
        print("  + create  → Terraform will call AWS CreateXxx API")
        print("  ~ update  → Terraform will call AWS UpdateXxx API (in-place, no downtime)")
        print("  - destroy → Terraform will call AWS DeleteXxx API")
        print("  -/+ replace → destroy then recreate (potential downtime)")
        return

    tfvars = TERRAFORM_DIR / "terraform.tfvars"
    if not tfvars.exists():
        print("\n  terraform.tfvars not found.")
        print("  Copy terraform/terraform.tfvars.example to terraform/terraform.tfvars")
        print("  and fill in your AWS account values, then re-run.")
        return

    print("\n  Running terraform init...")
    code, out = run(["terraform", "init", "-input=false"])
    print(out[:1000])
    if code != 0:
        print("  terraform init failed. Check your AWS credentials.")
        return

    print("\n  Running terraform plan (read-only, no changes will be made)...")
    code, out = run(["terraform", "plan", "-var-file=terraform.tfvars", "-input=false"])
    print(out[:3000])


def explain_outputs() -> None:
    """
    outputs.tf defines values Terraform prints after apply.
    Other tools (scripts, CI pipelines, Helm) read these to know:
      - What is the EKS cluster endpoint?
      - What is the RDS hostname?
      - What is the S3 bucket name?

    Read outputs any time with: terraform output -json
    """
    path = TERRAFORM_DIR / "outputs.tf"
    if path.exists():
        print("\n── outputs.tf ────────────────────────────────────────────────")
        print(path.read_text())


def run_walkthrough() -> None:
    print("\n── Stage 2: Terraform Walkthrough ──")

    print("""
CONCEPT RECAP
─────────────
  Terraform manages infrastructure the same way Git manages code:
    - Desired state  = .tf files  (like source code)
    - Current state  = .tfstate   (like the compiled binary)
    - Plan           = diff        (like git diff)
    - Apply          = commit      (like git push)

  The key insight: Terraform only changes what's different.
  Running `terraform apply` twice is safe — the second run does nothing
  because the actual state already matches the desired state.
""")

    explain_providers()
    explain_variables()
    explain_outputs()

    print("\n── terraform plan (live run if terraform is installed) ────────")
    show_plan_output()

    print("""
NEXT STEPS
──────────
  1. Copy terraform/terraform.tfvars.example → terraform/terraform.tfvars
  2. Fill in: aws_region, cluster_name, db_password
  3. terraform init
  4. terraform plan   ← review what will be created (~30 resources)
  5. terraform apply  ← creates VPC, EKS cluster, RDS, S3, IAM roles
  6. terraform output ← get the cluster endpoint and RDS hostname
  7. Move to Stage 3 (Kubernetes) — you'll use these outputs to deploy the pipeline
""")


if __name__ == "__main__":
    run_walkthrough()
