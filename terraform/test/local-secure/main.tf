# =============================================================================
# Local mTLS test harness: generate the CA + component certs + JWT keys with the
# security submodule, render security.toml with the core, then run a real
# master+volume+filer cluster with mTLS enabled on 127.0.0.1 and assert it works.
# Proves the generated security material is valid and accepted by weed.
# =============================================================================

terraform {
  required_version = ">= 1.3.0"
}

variable "weed_binary" {
  type    = string
  default = "/usr/bin/weed"
}

variable "workdir" {
  type    = string
  default = "/tmp/seaweedfs-tftest-secure"
}

module "security" {
  source          = "../../modules/security"
  internal_domain = "seaweedfs.internal"
  ip_sans         = ["127.0.0.1"]
  cert_dir        = "${var.workdir}/certs"
}

module "core" {
  source          = "../../modules/core"
  weed_binary     = var.weed_binary
  enable_security = true
  security        = module.security.core_security
  # We read security.toml back from the output and place it ourselves (under
  # $HOME/.seaweedfs), so keep it in the rendered file set.
  render_secret_files = true

  # High, distinct ports so this never collides with a running SeaweedFS or the
  # non-secure harness.
  master = {
    nodes              = { m0 = { address = "127.0.0.1", port = 29336, data_dir = "${var.workdir}/master-m0" } }
    election_timeout   = "3s"
    heartbeat_interval = "200ms"
  }
  volume = {
    nodes = { v0 = { address = "127.0.0.1", port = 28081, data_dirs = [{ path = "${var.workdir}/volume-v0", max_volumes = 20 }] } }
  }
  filer = {
    nodes = { f0 = { address = "127.0.0.1", port = 28889, data_dir = "${var.workdir}/filer-f0" } }
  }
}

output "cluster" {
  sensitive = true
  value = {
    for name, n in module.core.nodes : name => {
      role      = n.role
      http_port = n.ports.http
      data_dirs = n.data_dirs
      argv      = n.argv
      env       = n.env
    }
  }
}

output "security_toml" {
  sensitive = true
  value     = module.core.secret_files_by_node["master-m0"]["/etc/seaweedfs/security.toml"]
}

output "certs" {
  sensitive = true
  value     = [for m in module.security.secret_manifest : { path = m.path, mode = m.mode, content = module.security.secret_contents[m.key] }]
}
