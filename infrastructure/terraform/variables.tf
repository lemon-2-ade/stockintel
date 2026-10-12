variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "stockintel"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-1"
}

variable "vpc_cidr" {
  description = "CIDR block of the VPC."
  type        = string
  default     = "10.40.0.0/16"
}

variable "az_count" {
  description = "Availability zones (and MSK brokers). The Kafka topology assumes replication factor 3, so keep 3."
  type        = number
  default     = 3

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "az_count must be 2 or 3."
  }
}

variable "images" {
  description = "ECR repositories to create, one per service image."
  type        = list(string)
  default = [
    "market-producer",
    "stream-processor",
    "sinks",
    "api",
    "inference",
    "prediction-pipeline",
    "model-monitor",
    "frontend",
    "mlflow",
  ]
}

variable "db_instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.small"
}

variable "db_allocated_storage_gb" {
  description = "Initial RDS storage."
  type        = number
  default     = 50
}

variable "db_multi_az" {
  description = "Run a synchronous standby in a second zone."
  type        = bool
  default     = false
}

variable "deletion_protection" {
  description = "Protect stateful resources (RDS) from accidental deletion."
  type        = bool
  default     = true
}

variable "redis_node_type" {
  description = "ElastiCache node type."
  type        = string
  default     = "cache.t4g.small"
}

variable "msk_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.t3.small"
}

variable "msk_broker_volume_gb" {
  description = "EBS volume per broker."
  type        = number
  default     = 100
}

variable "tags" {
  description = "Extra tags for every resource."
  type        = map(string)
  default     = {}
}
