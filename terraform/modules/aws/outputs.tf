output "security_group_id" {
  description = "Cluster security group id."
  value       = aws_security_group.cluster.id
}

output "master_private_ips" {
  description = "Master private IPs keyed by node id."
  value       = { for k, v in var.masters : k => v.private_ip }
}

output "master_peers" {
  description = "Master peer list passed to the weed processes."
  value       = module.core.master_peers
}

output "filer_private_ips" {
  description = "Filer private IPs keyed by node id."
  value       = { for k, v in var.filers : k => v.private_ip }
}

output "s3_private_ips" {
  description = "Standalone S3 gateway private IPs keyed by node id."
  value       = { for k, v in var.s3_nodes : k => v.private_ip }
}

output "instance_ids" {
  description = "All instance ids keyed by role-node."
  value = merge(
    { for k, v in aws_instance.master : "master-${k}" => v.id },
    { for k, v in aws_instance.volume : "volume-${k}" => v.id },
    { for k, v in aws_instance.filer : "filer-${k}" => v.id },
    { for k, v in aws_instance.s3 : "s3-${k}" => v.id },
  )
}
