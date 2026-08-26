variable "aws_region" {
  description = "AWS region — locked to ap-south-1 (Mumbai) for latency and to avoid cross-region transfer charges"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Short name used as a prefix for all resources"
  type        = string
  default     = "retail-lakehouse"
}

variable "environment" {
  description = "Deployment environment tag"
  type        = string
  default     = "portfolio"
}
