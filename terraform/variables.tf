variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "eu-west-2"
}

variable "project_name" {
  description = "Prefix applied to all resource names"
  type        = string
  default     = "pipeline"
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  default     = "dev"
}

# ── Networking ────────────────────────────────────────────────
variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDRs for public subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDRs for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.10.0/24", "10.0.11.0/24"]
}

# ── RDS ───────────────────────────────────────────────────────
variable "db_instance_class" {
  description = "RDS instance type"
  type        = string
  default     = "db.t3.micro"
}

variable "db_name" {
  description = "Name of the initial database"
  type        = string
  default     = "warehouse_db"
}

variable "db_username" {
  description = "RDS master username"
  type        = string
  default     = "pipeline_user"
}

variable "db_password" {
  description = "RDS master password — set via TF_VAR_db_password or terraform.tfvars"
  type        = string
  sensitive   = true
  # No default — Terraform will error if not provided, preventing accidental exposure
}

# ── S3 ────────────────────────────────────────────────────────
variable "s3_raw_bucket_name" {
  description = "S3 bucket for raw (landing zone) data"
  type        = string
  default     = ""   # If empty, auto-generated as {project}-{account_id}-raw
}

variable "s3_processed_bucket_name" {
  description = "S3 bucket for processed data"
  type        = string
  default     = ""
}
