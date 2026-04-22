terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS region for KVS data volumes"
  type        = string
  default     = "us-east-1"
}

variable "kvs_worker_count" {
  description = "Number of KVS worker data volumes to create"
  type        = number
  default     = 20
}

variable "data_volume_size" {
  description = "Size in GB for each KVS worker data EBS volume"
  type        = number
  default     = 100
}

variable "availability_zone" {
  description = "Availability zone where all KVS workers (and data volumes) will run, e.g. us-east-1a"
  type        = string
  default     = "us-east-1a"
}

resource "aws_ebs_volume" "kvs_data" {
  count             = var.kvs_worker_count
  availability_zone = var.availability_zone
  size              = var.data_volume_size
  type              = "gp3"

  tags = {
    Name  = "kvs-data-${count.index}"
    Role  = "kvs-data"
    Index = tostring(count.index)
  }
}

output "kvs_data_volume_ids" {
  description = "IDs of the KVS data volumes (index-aligned: volume[0] for worker[0], etc.)"
  # Use explicit index-based iteration to guarantee order matches count.index
  value = [for i in range(var.kvs_worker_count) : aws_ebs_volume.kvs_data[i].id]
}



