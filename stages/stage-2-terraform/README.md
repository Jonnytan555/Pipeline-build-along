# Stage 2 — Terraform (AWS Infrastructure as Code)

Provisions the full AWS infrastructure for the pipeline using Terraform. All resources are declared in `.tf` files and managed as code — no manual console clicks.

## What it does

- Creates a VPC with public and private subnets across two availability zones
- Provisions S3 buckets (raw + processed) with versioning and encryption
- Deploys an RDS PostgreSQL 16 instance (`db.t3.micro`) in private subnets
- Creates IAM role + policy for EC2 → S3 access
- Sets up security groups, NAT Gateway, and route tables

## Files

| File | Purpose |
|---|---|
| `terraform_walkthrough.py` | Concept walkthrough script |
| *(actual `.tf` files live in `terraform/` at project root)* | |

See [terraform/](../../terraform/) for the live Terraform configuration.

## Provisioned resources

| Resource | ID / Name |
|---|---|
| VPC | `vpc-0abe1ff562786a315` (10.0.0.0/16) |
| Public subnets | 10.0.1.0/24, 10.0.2.0/24 (eu-west-2a/b) |
| Private subnets | 10.0.10.0/24, 10.0.11.0/24 |
| S3 raw bucket | `pipeline-033484685711-raw` |
| S3 processed bucket | `pipeline-033484685711-processed` |
| RDS endpoint | `pipeline-warehouse.c1062y0g24g2.eu-west-2.rds.amazonaws.com:5432` |
| IAM role | `pipeline-pipeline-role` |
| NAT Gateway | `nat-0f92dca2e24941b45` |

## Run

```bash
cd terraform/

# First time
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"

# Destroy when done (avoid NAT Gateway costs ~£1/day)
terraform destroy -var-file="terraform.tfvars"
```

Copy `terraform.tfvars.example` → `terraform.tfvars` and fill in your values.

## Required variables (`terraform.tfvars`)

```hcl
aws_region   = "eu-west-2"
db_password  = "your-password-here"
```

## Terraform workflow

```
terraform init    # download AWS provider plugin
      ↓
terraform plan    # diff: desired state (.tf) vs actual state (.tfstate)
      ↓
terraform apply   # execute the plan (creates/updates/destroys)
```

| Symbol in plan | Meaning |
|---|---|
| `+` | Create |
| `~` | Update in-place |
| `-` | Destroy |
| `-/+` | Replace (destroy + recreate — may cause downtime) |

## Cost notes

- **NAT Gateway** is ~£1/day — run `terraform destroy` when not in use
- **RDS db.t3.micro** is free tier eligible for the first 12 months (750 h/month)
- **S3** is negligible at dev data volumes

## Pipeline position

```
[Stage 2: Terraform creates AWS VPC/S3/RDS]
      ↓
Stage 3 (Kubernetes cluster deployed into this VPC)
Stage 4 (Great Expectations validates data in this pipeline)
```
