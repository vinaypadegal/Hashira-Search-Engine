output "kvs_coordinator_id" {
  description = "ID of the KVS coordinator instance"
  value       = aws_instance.kvs_coordinator.id
}


output "kvs_coordinator_public_dns" {
  description = "Public DNS of the KVS coordinator"
  value       = aws_instance.kvs_coordinator.public_dns
}

output "kvs_coordinator_private_ip" {
  description = "Private IP of the KVS coordinator (used by workers and Flame)"
  value       = aws_instance.kvs_coordinator.private_ip
}

output "kvs_worker_private_ips" {
  description = "Private IPs of all worker instances (all 10 run KVS on port 8001, first 6 also run Flame on ports 9001-9006)"
  value       = [for w in aws_instance.kvs_worker : w.private_ip]
}

output "kvs_worker_public_ips" {
  description = "Public IPs of all worker instances (all 10 run KVS on port 8001, first 6 also run Flame on ports 9001-9006)"
  value       = [for w in aws_instance.kvs_worker : w.public_ip]
}

output "worker_count" {
  description = "Total number of worker instances (10 total: all run KVS workers, first 6 also run Flame workers)"
  value       = 10
}

output "flame_coordinator_public_dns" {
  description = "Public DNS of the Flame coordinator"
  value       = aws_instance.flame_coordinator.public_dns
}

output "flame_coordinator_private_ip" {
  description = "Private IP of the Flame coordinator"
  value       = aws_instance.flame_coordinator.private_ip
}

output "flame_coordinator_public_ip" {
  description = "Public IP of the Flame coordinator"
  value       = aws_instance.flame_coordinator.public_ip
}

output "kvs_coordinator_public_ip" {
  description = "Public IP of the KVS coordinator"
  value       = aws_instance.kvs_coordinator.public_ip
}

output "worker_instance_ids" {
  description = "Instance IDs of all worker instances (all 10 run KVS workers, first 6 also run Flame workers)"
  value       = [for w in aws_instance.kvs_worker : w.id]
}

output "worker_instance_types" {
  description = "Instance type used for workers"
  value       = "i3.medium"
}

output "storage_configuration" {
  description = "Storage configuration for worker data volumes (configured in kvs-data-terraform)"
  value = {
    volume_type     = "gp3"
    volume_size_gb  = 100
    iops            = "3000 (default)"
    throughput_mbps = "125 (default)"
    note            = "Using default GP3 performance. Configured in kvs-data-terraform/main.tf"
  }
}

output "deployment_summary" {
  description = "Summary of the deployment configuration"
  value = {
    architecture           = "10 i3.medium instances: all run KVS workers, first 6 also run Flame workers"
    worker_count           = 10
    kvs_worker_count       = 10
    flame_worker_count     = 6
    worker_instance_type   = "i3.medium"
    coordinator_type       = "t3.small"
    kvs_port              = 8001
    flame_port_range      = "9001-9006 (first 6 workers only)"
    flame_port            = 9001
    storage_per_worker_gb = 100
    storage_type          = "gp3 (default performance)"
  }
}

