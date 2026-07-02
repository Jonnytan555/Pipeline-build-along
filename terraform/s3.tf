data "aws_caller_identity" "current" {}

locals {
  raw_bucket_name       = var.s3_raw_bucket_name != "" ? var.s3_raw_bucket_name : "${var.project_name}-${data.aws_caller_identity.current.account_id}-raw"
  processed_bucket_name = var.s3_processed_bucket_name != "" ? var.s3_processed_bucket_name : "${var.project_name}-${data.aws_caller_identity.current.account_id}-processed"
}

# ── Raw data lake (landing zone — MinIO replacement) ──────────
resource "aws_s3_bucket" "raw" {
  bucket        = local.raw_bucket_name
  force_destroy = var.environment != "prod"

  tags = { Name = local.raw_bucket_name, Layer = "raw" }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Processed / curated zone ──────────────────────────────────
resource "aws_s3_bucket" "processed" {
  bucket        = local.processed_bucket_name
  force_destroy = var.environment != "prod"

  tags = { Name = local.processed_bucket_name, Layer = "processed" }
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
