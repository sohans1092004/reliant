output "region" {
  value = var.region
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "ecr_repository_url" {
  value = aws_ecr_repository.app.repository_url
}

output "db_password_secret_arn" {
  description = "Secrets Manager secret RDS manages for the master password"
  value       = aws_db_instance.postgres.master_user_secret[0].secret_arn
}

output "kms_key_alias" {
  value = aws_kms_alias.app.name
}

output "archive_bucket" {
  value = aws_s3_bucket.archive.id
}

output "service_role_arns" {
  value = { for k, r in aws_iam_role.service : k => r.arn }
}

# Ready to save as k8s/aws/config.env:  terraform output -raw k8s_config_env > ../../k8s/aws/config.env
output "k8s_config_env" {
  value = <<-EOT
    DB_HOST=${aws_db_instance.postgres.address}
    DB_PORT=${aws_db_instance.postgres.port}
    DB_USER=${aws_db_instance.postgres.username}
    DB_NAME=${aws_db_instance.postgres.db_name}
    DB_SSL_CA=/etc/rds/global-bundle.pem
    KAFKA_BROKER=${aws_msk_cluster.kafka.bootstrap_brokers_tls}
    KAFKA_SSL=true
    REDIS_URL=rediss://${aws_elasticache_replication_group.redis.primary_endpoint_address}:6379
    AWS_REGION=${var.region}
    KMS_KEY_ID=${aws_kms_alias.app.name}
    ARCHIVE_BUCKET=${aws_s3_bucket.archive.id}
    RETRY_FAST_MODE=false
    ALLOW_PRIVATE_ENDPOINTS=false
  EOT
}
