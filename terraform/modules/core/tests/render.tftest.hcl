# Plan-level tests for the core renderer. No cloud credentials required.
#   cd terraform/modules/core && tofu test

variables {
  weed_binary = "/usr/bin/weed"
}

# ---------------------------------------------------------------------------
run "master_peers_and_count" {
  command = plan
  variables {
    master = {
      nodes = {
        m0 = { address = "10.0.0.10" }
        m1 = { address = "10.0.0.11" }
        m2 = { address = "10.0.0.12" }
      }
    }
    volume = { enabled = false }
    filer  = { enabled = false }
  }
  assert {
    condition     = output.master_peers == "10.0.0.10:9333,10.0.0.11:9333,10.0.0.12:9333"
    error_message = "master -peers list is wrong"
  }
  assert {
    condition     = length(output.node_names) == 3
    error_message = "expected exactly 3 master nodes"
  }
}

# ---------------------------------------------------------------------------
run "volume_uses_mserver_not_master" {
  command = plan
  variables {
    master = { nodes = { m0 = { address = "10.0.0.10" } } }
    volume = { nodes = { v0 = { address = "10.0.0.20", rack = "rack-a", data_center = "dc1" } } }
    filer  = { enabled = false }
  }
  assert {
    condition     = contains(output.nodes["volume-v0"].argv, "-mserver=10.0.0.10:9333")
    error_message = "volume must pass the master list via -mserver (verified flag), not -master"
  }
  assert {
    condition     = !contains(output.nodes["volume-v0"].argv, "-master=10.0.0.10:9333")
    error_message = "volume should not use -master for the master list"
  }
  assert {
    condition     = contains(output.nodes["volume-v0"].argv, "-rack=rack-a") && contains(output.nodes["volume-v0"].argv, "-dataCenter=dc1")
    error_message = "per-node rack/dataCenter must render"
  }
}

# ---------------------------------------------------------------------------
run "filer_uses_master_and_store_dir" {
  command = plan
  variables {
    master = { nodes = { m0 = { address = "10.0.0.10" } } }
    volume = { enabled = false }
    filer  = { nodes = { f0 = { address = "10.0.0.30", data_dir = "/srv/filer" } } }
  }
  assert {
    condition     = contains(output.nodes["filer-f0"].argv, "-master=10.0.0.10:9333")
    error_message = "filer must use -master for the master list"
  }
  assert {
    condition     = contains(output.nodes["filer-f0"].argv, "-defaultStoreDir=/srv/filer")
    error_message = "filer leveldb2 store dir must render via -defaultStoreDir"
  }
}

# ---------------------------------------------------------------------------
run "metrics_gated_off_by_default" {
  command = plan
  variables {
    master = { nodes = { m0 = { address = "10.0.0.10" } } }
    volume = { enabled = false }
    filer  = { enabled = false }
  }
  assert {
    condition     = length([for a in output.nodes["master-m0"].argv : a if startswith(a, "-metricsPort")]) == 0
    error_message = "metrics port must NOT be bound when monitoring is disabled"
  }
}

run "metrics_on_when_enabled" {
  command = plan
  variables {
    monitoring_enabled = true
    master             = { nodes = { m0 = { address = "10.0.0.10" } }, metrics_port = 9327 }
    volume             = { enabled = false }
    filer              = { enabled = false }
  }
  assert {
    condition     = contains(output.nodes["master-m0"].argv, "-metricsPort=9327")
    error_message = "metrics port must bind when monitoring is enabled"
  }
}

# ---------------------------------------------------------------------------
run "security_toml_off_by_default" {
  command = plan
  variables {
    master = { nodes = { m0 = { address = "10.0.0.10" } } }
    volume = { enabled = false }
    filer  = { enabled = false }
  }
  assert {
    condition     = length(keys(output.nodes["master-m0"].config_files)) == 0
    error_message = "security.toml must not render when security is off and no JWT keys set"
  }
}

run "security_toml_on_with_mtls" {
  command = plan
  variables {
    enable_security = true
    security = {
      allowed_wildcard_domain = ".seaweedfs.internal"
      jwt_signing_key         = "test-signing-key-not-a-real-secret-0123456789"
    }
    master = { nodes = { m0 = { address = "10.0.0.10" } } }
    volume = { enabled = false }
    filer  = { enabled = false }
  }
  assert {
    condition     = contains(keys(output.nodes["master-m0"].config_files), "/etc/seaweedfs/security.toml")
    error_message = "security.toml must render when enable_security is true"
  }
}

# ---------------------------------------------------------------------------
run "all_in_one_renders_server" {
  command = plan
  variables {
    master = { enabled = false }
    volume = { enabled = false }
    filer  = { enabled = false }
    all_in_one = {
      enabled = true
      nodes   = { a0 = { address = "10.0.0.40" } }
      s3      = { enabled = true }
    }
  }
  assert {
    condition     = contains(output.nodes["all-in-one-a0"].argv, "server")
    error_message = "all-in-one must invoke the weed server subcommand"
  }
  assert {
    condition     = contains(output.nodes["all-in-one-a0"].argv, "-s3") && contains(output.nodes["all-in-one-a0"].argv, "-s3.port=8333")
    error_message = "all-in-one S3 must render when enabled"
  }
}

# ---------------------------------------------------------------------------
run "mount_fetch_and_secret_delivery" {
  command = plan
  variables {
    enable_security     = true
    render_secret_files = false
    boot_fetch_script   = "#!/usr/bin/env bash\necho fetch\n"
    security = {
      jwt_signing_key = "test-signing-key-not-a-real-secret-0123456789"
    }
    master = { nodes = { m0 = { address = "10.0.0.10", disk_mounts = [{ mountpoint = "/data", fstype = "xfs" }] } } }
    volume = { enabled = false }
    filer  = { enabled = false }
  }
  assert {
    condition     = strcontains(output.nodes["master-m0"].cloud_init, "/opt/seaweedfs/mount-disks.sh")
    error_message = "cloud-init must write+run the mount script when disk_mounts is set"
  }
  assert {
    condition     = strcontains(output.nodes["master-m0"].cloud_init, "/opt/seaweedfs/fetch-secrets.sh")
    error_message = "cloud-init must write+run the fetch script when boot_fetch_script is set"
  }
  assert {
    condition     = !strcontains(output.nodes["master-m0"].cloud_init, "[jwt.signing]")
    error_message = "security.toml must NOT be inlined in cloud-init when render_secret_files=false"
  }
  assert {
    condition     = contains(keys(output.nodes["master-m0"].secret_files), "/etc/seaweedfs/security.toml")
    error_message = "secret_files must expose security.toml for secret-store delivery"
  }
}

run "mount_script_absent_without_disks" {
  command = plan
  variables {
    master = { nodes = { m0 = { address = "10.0.0.10" } } }
    volume = { enabled = false }
    filer  = { enabled = false }
  }
  assert {
    condition     = !strcontains(output.nodes["master-m0"].cloud_init, "mount-disks.sh")
    error_message = "no mount script should render when the node declares no disk_mounts"
  }
}

run "s3_standalone_targets_filer" {
  command = plan
  variables {
    master = { nodes = { m0 = { address = "10.0.0.10" } } }
    volume = { enabled = false }
    filer  = { nodes = { f0 = { address = "10.0.0.30" } } }
    s3     = { enabled = true, nodes = { s0 = { address = "10.0.0.50" } } }
  }
  assert {
    condition     = contains(output.nodes["s3-s0"].argv, "-filer=10.0.0.30:8888")
    error_message = "standalone S3 must point -filer at the first filer node"
  }
  assert {
    condition     = contains(output.nodes["s3-s0"].argv, "-config=/etc/seaweedfs/s3_config.json")
    error_message = "standalone S3 must reference the rendered config path"
  }
}
