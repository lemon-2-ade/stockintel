# Provisioned MSK with SASL/SCRAM rather than MSK Serverless: Serverless allows
# IAM authentication only, and the services speak PLAIN/SCRAM (KafkaSettings).
# Topics are created by `python -m shared.kafka.provision` with replication
# factor 3 and min.insync.replicas 2 (KAFKA_TOPIC_REPLICATION_FACTOR /
# KAFKA_TOPIC_MIN_INSYNC_REPLICAS), which is why three brokers are the default.

resource "aws_kms_key" "kafka" {
  description         = "${local.name} MSK SCRAM credentials"
  enable_key_rotation = true
}

resource "random_password" "kafka_app" {
  length  = 40
  special = false
}

# MSK requires the secret name to start with AmazonMSK_ and to use a
# customer-managed KMS key.
resource "aws_secretsmanager_secret" "kafka_app" {
  name        = "AmazonMSK_${local.name}_app"
  description = "SASL/SCRAM credentials the services use for Kafka"
  kms_key_id  = aws_kms_key.kafka.arn
}

resource "aws_secretsmanager_secret_version" "kafka_app" {
  secret_id = aws_secretsmanager_secret.kafka_app.id
  secret_string = jsonencode({
    username = "stockintel"
    password = random_password.kafka_app.result
  })
}

resource "aws_msk_configuration" "main" {
  name           = local.name
  kafka_versions = ["3.8.x"]

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    unclean.leader.election.enable=false
  PROPS
}

resource "aws_msk_cluster" "main" {
  cluster_name           = local.name
  kafka_version          = "3.8.x"
  number_of_broker_nodes = var.az_count

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.kafka.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.msk_broker_volume_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  client_authentication {
    sasl {
      scram = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  enhanced_monitoring = "PER_TOPIC_PER_BROKER"

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }
}

resource "aws_msk_scram_secret_association" "app" {
  cluster_arn     = aws_msk_cluster.main.arn
  secret_arn_list = [aws_secretsmanager_secret.kafka_app.arn]

  depends_on = [aws_secretsmanager_secret_version.kafka_app]
}
