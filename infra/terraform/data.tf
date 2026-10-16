# Postgres, Redis and Kafka, each in the private database subnets and reachable only from
# the EKS nodes' security group, each encrypted in transit and at rest.

locals {
  data_subnets = module.vpc.database_subnets
  from_nodes   = module.eks.node_security_group_id
}

resource "aws_security_group" "postgres" {
  name_prefix = "${var.name}-postgres-"
  vpc_id      = module.vpc.vpc_id
  ingress {
    description     = "Postgres from EKS nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [local.from_nodes]
  }
}

resource "aws_security_group" "redis" {
  name_prefix = "${var.name}-redis-"
  vpc_id      = module.vpc.vpc_id
  ingress {
    description     = "Redis (TLS) from EKS nodes"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [local.from_nodes]
  }
}

resource "aws_security_group" "kafka" {
  name_prefix = "${var.name}-kafka-"
  vpc_id      = module.vpc.vpc_id
  ingress {
    description     = "Kafka TLS listener from EKS nodes"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [local.from_nodes]
  }
}

# --- Postgres: the only source of truth (ADR-001), so it gets the most protection ---

resource "aws_db_subnet_group" "postgres" {
  name       = "${var.name}-postgres"
  subnet_ids = local.data_subnets
}

resource "aws_db_parameter_group" "postgres" {
  name   = "${var.name}-postgres16"
  family = "postgres16"
  parameter {
    name  = "rds.force_ssl" # the app verifies the server certificate (DB_SSL_CA)
    value = "1"
  }
}

resource "aws_db_instance" "postgres" {
  identifier     = var.name
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  db_name  = "reliant"
  username = "reliant"
  # Generated and rotated by RDS in Secrets Manager: never in this config or its state.
  manage_master_user_password = true

  allocated_storage     = 50
  max_allocated_storage = 500
  storage_type          = "gp3"
  storage_encrypted     = true

  multi_az               = var.db_multi_az
  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  parameter_group_name   = aws_db_parameter_group.postgres.name
  publicly_accessible    = false

  backup_retention_period   = var.db_backup_retention_days
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "${var.name}-final"
  copy_tags_to_snapshot     = true
}

resource "aws_db_instance" "postgres_replica" {
  count               = var.db_read_replica ? 1 : 0
  identifier          = "${var.name}-replica"
  replicate_source_db = aws_db_instance.postgres.identifier
  instance_class      = var.db_instance_class
  storage_encrypted   = true
  skip_final_snapshot = true
  publicly_accessible = false
}

# --- Redis: breaker state and rate-limit buckets only, all disposable (spec §7) ---

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${var.name}-redis"
  subnet_ids = local.data_subnets
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = var.name
  description          = "Reliant circuit breakers and rate limits"
  engine               = "redis"
  engine_version       = "7.1"
  node_type            = var.redis_node_type

  num_cache_clusters         = 2 # primary + replica in another AZ
  automatic_failover_enabled = true
  multi_az_enabled           = true

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [aws_security_group.redis.id]

  transit_encryption_enabled = true # the app connects with rediss://
  at_rest_encryption_enabled = true
}

# --- Kafka (MSK): the ordered intake buffer (ADR-004); managed, per ADR-003 ---

resource "aws_msk_configuration" "kafka" {
  name           = var.name
  kafka_versions = [var.kafka_version]
  # acks=all (the idempotent producer) only means "on 2 of 3 brokers" with this ISR floor.
  server_properties = <<-EOT
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    unclean.leader.election.enable=false
  EOT
}

resource "aws_msk_cluster" "kafka" {
  cluster_name           = var.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 3 # one per AZ

  broker_node_group_info {
    instance_type   = var.kafka_instance_type
    client_subnets  = local.data_subnets
    security_groups = [aws_security_group.kafka.id]
    storage_info {
      ebs_storage_info {
        volume_size = var.kafka_volume_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.kafka.arn
    revision = aws_msk_configuration.kafka.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS" # the app sets KAFKA_SSL=true
      in_cluster    = true
    }
  }
}
