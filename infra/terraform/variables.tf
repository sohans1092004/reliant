variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  description = "Prefix for every resource name"
  type        = string
  default     = "reliant"
}

variable "localstack" {
  description = "Send every AWS call to LocalStack (localhost:4566) instead of a real account"
  type        = bool
  default     = false
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "single_nat_gateway" {
  description = "One NAT gateway for all AZs: cheaper, but an AZ outage cuts the other AZs' egress"
  type        = bool
  default     = true
}

variable "cluster_admin_cidrs" {
  description = "Networks allowed to reach the EKS API endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "kubernetes_version" {
  type    = string
  default = "1.33"
}

variable "node_instance_types" {
  type    = list(string)
  default = ["t3.large"]
}

variable "node_count" {
  description = "Managed node group size: min, desired, max"
  type        = object({ min = number, desired = number, max = number })
  default     = { min = 2, desired = 3, max = 6 }
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.medium"
}

variable "db_multi_az" {
  description = "Standby replica in a second AZ with automatic failover"
  type        = bool
  default     = true
}

variable "db_backup_retention_days" {
  description = "Point-in-time recovery window (RPO is ~5 minutes within it)"
  type        = number
  default     = 7
}

variable "db_read_replica" {
  description = "Read replica for dashboard queries (spec §13). Off: the dashboard meets its latency target on the primary, and the app has no read-host setting yet"
  type        = bool
  default     = false
}

variable "redis_node_type" {
  type    = string
  default = "cache.t4g.small"
}

variable "kafka_version" {
  type    = string
  default = "3.8.x"
}

variable "kafka_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "kafka_volume_gb" {
  type    = number
  default = 100
}

variable "archive_expiry_days" {
  description = "How long retention archives are kept in S3"
  type        = number
  default     = 730
}

variable "k8s_namespace" {
  description = "Namespace the app runs in (k8s/aws overlay); IAM roles are bound to its service accounts"
  type        = string
  default     = "reliant"
}
