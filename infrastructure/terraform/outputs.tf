output "ecr_repository_urls" {
  description = "Push each service image here (tags are immutable)."
  value       = { for name, repo in aws_ecr_repository.image : name => repo.repository_url }
}

output "app_security_group_id" {
  description = "Attach to the workloads that run the services."
  value       = aws_security_group.app.id
}

output "private_subnet_ids" {
  description = "Subnets for the compute layer."
  value       = aws_subnet.private[*].id
}

output "postgres_endpoint" {
  description = "POSTGRES_HOST / POSTGRES_PORT."
  value       = aws_db_instance.postgres.endpoint
}

output "postgres_master_secret_arn" {
  description = "Secrets Manager secret holding the RDS master credentials."
  value       = aws_db_instance.postgres.master_user_secret[0].secret_arn
}

output "redis_primary_endpoint" {
  description = "REDIS_HOST (TLS on, port 6379)."
  value       = aws_elasticache_replication_group.redis.primary_endpoint_address
}

output "redis_auth_secret_arn" {
  description = "Secrets Manager secret holding the Redis AUTH token (REDIS_PASSWORD)."
  value       = aws_secretsmanager_secret.redis.arn
}

output "kafka_bootstrap_brokers_sasl_scram" {
  description = "KAFKA_BOOTSTRAP_SERVERS with KAFKA_SECURITY_PROTOCOL=SASL_SSL, KAFKA_SASL_MECHANISM=SCRAM-SHA-512."
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_scram
}

output "kafka_credentials_secret_arn" {
  description = "Secrets Manager secret with the SCRAM username and password."
  value       = aws_secretsmanager_secret.kafka_app.arn
}
