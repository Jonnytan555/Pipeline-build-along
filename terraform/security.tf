# ── Security Groups ───────────────────────────────────────────

# RDS — only reachable from within the VPC
resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Allow Postgres from within VPC"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Postgres from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-rds-sg" }
}

# ── IAM — pipeline execution role ────────────────────────────
resource "aws_iam_role" "pipeline" {
  name = "${var.project_name}-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# S3 access — read/write to raw and processed buckets
resource "aws_iam_policy" "s3_pipeline" {
  name        = "${var.project_name}-s3-policy"
  description = "R/W access to pipeline S3 buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "pipeline_s3" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.s3_pipeline.arn
}

# Instance profile so EC2 / ECS tasks can assume the role
resource "aws_iam_instance_profile" "pipeline" {
  name = "${var.project_name}-instance-profile"
  role = aws_iam_role.pipeline.name
}
