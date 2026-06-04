# =============================================================================
# Local test harness: render a small SeaweedFS cluster with the core module
# and run it as real `weed` processes on 127.0.0.1 (no cloud, no docker).
#
# 3 masters (quorum) + 1 volume + 1 filer + 1 standalone S3, each on a distinct
# port. run_local_cluster.sh consumes the `cluster` output, launches the weed
# processes from the rendered argv, and asserts the cluster actually works.
# =============================================================================

terraform {
  required_version = ">= 1.3.0"
}

variable "weed_binary" {
  description = "Path to the weed executable used for the local cluster."
  type        = string
  default     = "/usr/bin/weed"
}

variable "workdir" {
  description = "Scratch directory for per-node data dirs and config files."
  type        = string
  default     = "/tmp/seaweedfs-tftest"
}

module "core" {
  source = "../../modules/core"

  weed_binary        = var.weed_binary
  monitoring_enabled = false
  enable_security    = false

  # High port range so the harness does not collide with a SeaweedFS cluster
  # that may already be running on this dev machine (default 9333/8080/8888/8333).
  master = {
    nodes = {
      m0 = { address = "127.0.0.1", port = 29333, data_dir = "${var.workdir}/master-m0" }
      m1 = { address = "127.0.0.1", port = 29334, data_dir = "${var.workdir}/master-m1" }
      m2 = { address = "127.0.0.1", port = 29335, data_dir = "${var.workdir}/master-m2" }
    }
    # Tighten election so a 3-node local quorum converges quickly.
    election_timeout   = "3s"
    heartbeat_interval = "200ms"
  }

  volume = {
    nodes = {
      v0 = {
        address   = "127.0.0.1"
        port      = 28080
        rack      = "rack-a"
        data_dirs = [{ path = "${var.workdir}/volume-v0", max_volumes = 20 }]
      }
    }
  }

  filer = {
    nodes = {
      f0 = { address = "127.0.0.1", port = 28888, data_dir = "${var.workdir}/filer-f0" }
    }
  }

  s3 = {
    enabled = true
    nodes   = { s0 = { address = "127.0.0.1", port = 28333 } }
    # this weed build starts an Iceberg REST catalog on 8181 by default; disable
    # it so the test does not collide with a cluster already using that port.
    iceberg_port = 0
    config_path  = "${var.workdir}/s3_config.json"
  }

  # Anonymous identity so the smoke test can PUT/GET without sigv4 signing.
  s3_identities = [{
    name       = "anonymous"
    access_key = ""
    secret_key = ""
    actions    = ["Admin", "Read", "Write", "List", "Tagging"]
  }]
}

output "cluster" {
  description = "Node specs consumed by run_local_cluster.sh (read via `tofu output -json`, which emits values even when sensitive)."
  sensitive   = true
  value = {
    for name, n in module.core.nodes : name => {
      role         = n.role
      address      = n.address
      http_port    = n.ports.http
      data_dirs    = n.data_dirs
      argv         = n.argv
      env          = n.env
      config_files = n.config_files
      exec_start   = n.exec_start
    }
  }
}

output "master_peers" {
  value = module.core.master_peers
}
