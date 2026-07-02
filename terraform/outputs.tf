output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (use for RDS, EKS nodes)"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnet IDs (use for load balancers)"
  value       = aws_subnet.public[*].id
}

output "rds_endpoint" {
  description = "RDS hostname — use as POSTGRES_HOST in your .env"
  value       = aws_db_instance.warehouse.address
}

output "rds_port" {
  description = "RDS port"
  value       = aws_db_instance.warehouse.port
}

output "s3_raw_bucket" {
  description = "S3 raw bucket name — use as MINIO_BUCKET_RAW replacement"
  value       = aws_s3_bucket.raw.bucket
}

output "s3_processed_bucket" {
  description = "S3 processed bucket name"
  value       = aws_s3_bucket.processed.bucket
}

output "pipeline_role_arn" {
  description = "IAM role ARN for the pipeline — attach to EC2 / ECS tasks"
  value       = aws_iam_role.pipeline.arn
}
