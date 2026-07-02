# ── RDS Subnet Group (must span 2+ AZs) ──────────────────────
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id

  tags = { Name = "${var.project_name}-db-subnet-group" }
}

# ── RDS PostgreSQL Instance ───────────────────────────────────
resource "aws_db_instance" "warehouse" {
  identifier = "${var.project_name}-warehouse"

  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  # Storage
  allocated_storage     = 20
  max_allocated_storage = 100   # autoscaling upper limit
  storage_type          = "gp3"
  storage_encrypted     = true

  # Network
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false

  # Availability
  multi_az            = false   # set true in prod for failover
  skip_final_snapshot = var.environment != "prod"

  # Backups — 0 disables automated backups (required for free tier)
  backup_retention_period = 0
  maintenance_window      = "Mon:04:00-Mon:05:00"

  # Performance Insights (free for 7 days retention)
  performance_insights_enabled          = true
  performance_insights_retention_period = 7

  tags = { Name = "${var.project_name}-warehouse" }
}
