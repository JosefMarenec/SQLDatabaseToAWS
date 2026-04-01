variable "project" {
  description = "Project name prefix"
  type        = string
  default     = "ecom-api"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "rds_host" {
  description = "RDS PostgreSQL host endpoint"
  type        = string
}

variable "rds_port" {
  description = "RDS PostgreSQL port"
  type        = number
  default     = 5432
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "ecommerce"
}

variable "rds_secret_name" {
  description = "Secrets Manager secret name for RDS credentials"
  type        = string
}

variable "vpc_subnet_ids" {
  description = "List of private subnet IDs for Lambda VPC config"
  type        = list(string)
}

variable "vpc_security_group_ids" {
  description = "Security group IDs for Lambda functions"
  type        = list(string)
}

variable "lambda_runtime" {
  description = "Lambda Python runtime"
  type        = string
  default     = "python3.11"
}

variable "lambda_memory_mb" {
  description = "Memory (MB) allocated to each Lambda function"
  type        = number
  default     = 256
}

variable "lambda_timeout_s" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 30
}

variable "log_level" {
  description = "Python log level for Lambda functions"
  type        = string
  default     = "INFO"
}
