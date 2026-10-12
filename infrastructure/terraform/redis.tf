resource "random_password" "redis" {
  length  = 48
  special = false # ElastiCache auth tokens restrict punctuation
}

resource "aws_secretsmanager_secret" "redis" {
  name_prefix = "${local.name}/redis-"
  description = "Redis AUTH token for the live cache"
}

resource "aws_secretsmanager_secret_version" "redis" {
  secret_id     = aws_secretsmanager_secret.redis.id
  secret_string = random_password.redis.result
}

resource "aws_elasticache_subnet_group" "main" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

# The cache only holds the latest state, which the sinks rebuild from Kafka,
# so no persistence is configured and one replica is enough for availability.
resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${local.name}-redis"
  description          = "Latest-state cache and live-update pub/sub"

  engine         = "redis"
  engine_version = "7.1"
  node_type      = var.redis_node_type
  port           = 6379

  num_cache_clusters         = 2
  automatic_failover_enabled = true
  multi_az_enabled           = true

  subnet_group_name          = aws_elasticache_subnet_group.main.name
  security_group_ids         = [aws_security_group.redis.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = random_password.redis.result
}
