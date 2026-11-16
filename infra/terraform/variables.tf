variable "name_prefix" {
  description = "Prefix for every resource name, e.g. \"acme-servicing-dev\"."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]{3,32}$", var.name_prefix))
    error_message = "name_prefix must be 3-32 characters of lowercase letters, digits and hyphens."
  }
}

variable "vpc_id" {
  description = "VPC that hosts the MSK Serverless cluster and its clients."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets for MSK Serverless, in at least two availability zones."
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "MSK Serverless needs subnets in at least two availability zones."
  }
}

variable "client_security_group_ids" {
  description = "Security groups of the Kafka clients (Kafka Connect workers, Spark) allowed to reach the brokers."
  type        = list(string)
  default     = []
}

variable "noncurrent_version_expiration_days" {
  description = "Days to keep noncurrent S3 object versions. Iceberg snapshot expiry removes files; this bounds how long the removed versions linger."
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}
