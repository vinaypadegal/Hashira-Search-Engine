variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Optional AWS shared credentials profile to use"
  type        = string
  default     = ""
}

variable "ami_id" {
  description = "AMI ID for the EC2 instance (must exist in the chosen region)"
  type        = string
}

variable "instance_type_worker" {
  description = "EC2 instance type for workers (runs both KVS and Flame workers)"
  type        = string
  default     = "i3.large"
}

variable "instance_type_coord" {
  description = "EC2 instance type for coordinators (handles high load)"
  type        = string
  default     = "t3.small"  # Upgraded from t3.large for better handling of 200+ concurrent jobs
}


variable "ssh_key_name" {
  description = "Name of an existing AWS EC2 key pair for SSH access"
  type        = string
}

variable "security_group_ids" {
  description = "List of security group IDs to attach to the instance"
  type        = list(string)
  default     = []
}

variable "ssh_allowed_cidr" {
  description = "CIDR range allowed to SSH into the instances (e.g. your IP/32). Use 0.0.0.0/0 for anywhere (not recommended for prod)."
  type        = string
  default     = "0.0.0.0/0"
}

variable "subnet_id" {
  description = "Subnet ID in which to place the EC2 instance"
  type        = string
  default     = ""
}

variable "worker_availability_zone" {
  description = "Availability zone for all EC2 instances (used when subnet_id is not provided)"
  type        = string
  default     = "us-east-1a"
}


variable "extra_tags" {
  description = "Additional tags to apply to the EC2 instance"
  type        = map(string)
  default     = {}
}




variable "kvs_worker_count" {
  description = "Number of worker instances to run"
  type        = number
  default     = 10
}

# variable "flame_worker_count" {
#   description = "Number of Flame worker instances to run"
#   type        = number
#   default     = 15
# }

variable "flame_instance_name" {
  description = "Base name tag for Flame worker EC2 instances"
  type        = string
  default     = "hashira-flame-worker"
}


variable "kvs_instance_name" {
  description = "Name tag for the EC2 instance"
  type        = string
  default     = "hashira-kvs-worker"
}

variable "download_url" {
  description = "URL to download the project zip file"
  type        = string
  default     = "https://555-public.s3.us-east-1.amazonaws.com/Fa25-CIS5550-Project-Hashira-kvs_optimization.zip"
}

variable "project_folder_name" {
  description = "Name to rename the extracted project folder to"
  type        = string
  default     = "Fa25-CIS5550-Project-Hashira-kvs_optimization"
}

variable "kvs_data_folder_name" {
  description = "Name of the folder to store the KVS data in"
  type        = string
  default     = "batchJobTest3"
}