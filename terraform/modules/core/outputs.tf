output "nodes" {
  description = "Per-node rendered artifacts keyed by node name. Each value: role, name, address, ports, data_dir(s), argv (list, after the weed binary), exec_start, env, config_files, systemd_unit, cloud_init."
  value       = local.nodes
  sensitive   = true # config_files/env may carry JWT keys or S3 identities
}

output "master_peers" {
  description = "Comma-separated master peer list (address:port), as passed to -peers / -mserver / -master."
  value       = local.master_peers
}

output "first_filer" {
  description = "address:port of the lexically-first filer node (default S3 target)."
  value       = local.first_filer
}

output "node_names" {
  description = "List of rendered node names (non-sensitive)."
  value       = sort(keys(local.nodes))
}

output "cloud_init_by_node" {
  description = "Map of node name -> rendered cloud-init user_data (for the per-cloud wrappers)."
  value       = { for name, n in local.nodes : name => n.cloud_init }
  sensitive   = true
}

output "secret_files_by_node" {
  description = "Map of node name -> {path => content} of secret-bearing files (security.toml, S3 identity JSON). Wrappers push these to a secret store and fetch them at boot when render_secret_files=false."
  value       = { for name, n in local.nodes : name => n.secret_files }
  sensitive   = true
}
