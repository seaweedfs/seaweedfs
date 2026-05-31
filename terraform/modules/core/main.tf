# =============================================================================
# SeaweedFS Terraform core - pure renderer (zero cloud resources).
#
# Computes, per node: the verified `weed` argv, the full systemd ExecStart,
# config files, environment, and rendered systemd unit + cloud-init.
# =============================================================================

locals {
  # ---- shared security.toml (rendered when mTLS on OR any JWT key set) ----
  jwt_any = anytrue([
    var.security.jwt_signing_key != "",
    var.security.jwt_signing_read_key != "",
    var.security.jwt_filer_signing_key != "",
    var.security.jwt_filer_signing_read_key != "",
  ])
  render_security = var.enable_security || local.jwt_any
  security_toml = local.render_security ? templatefile("${path.module}/templates/security.toml.tftpl", {
    enable_security            = var.enable_security
    cert_dir                   = var.security.cert_dir
    allowed_wildcard_domain    = var.security.allowed_wildcard_domain
    allowed_common_names       = var.security.allowed_common_names
    jwt_signing_key            = var.security.jwt_signing_key
    jwt_signing_read_key       = var.security.jwt_signing_read_key
    jwt_filer_signing_key      = var.security.jwt_filer_signing_key
    jwt_filer_signing_read_key = var.security.jwt_filer_signing_read_key
  }) : ""
  security_files = local.render_security ? { "/etc/seaweedfs/security.toml" = local.security_toml } : {}

  # ---- S3 identity JSON (non-admin identities only) ----
  # Empty access_key => anonymous identity (no credentials block), per SeaweedFS
  # convention where the identity literally named "anonymous" grants anon access.
  s3_config_json = jsonencode({
    identities = [for id in var.s3_identities : {
      name        = id.name
      credentials = id.access_key != "" ? [{ accessKey = id.access_key, secretKey = id.secret_key }] : []
      actions     = id.actions
    }]
  })

  # ---- global flags (before the subcommand) ----
  global_flags = compact([
    format("-v=%d", var.log_level),
    var.log_to_stderr ? "-logtostderr=true" : "",
  ])

  # ---- master peers (sorted by key for determinism) ----
  master_keys = sort(keys(var.master.nodes))
  master_peer_list = [for k in local.master_keys :
    format("%s:%d", var.master.nodes[k].address, coalesce(var.master.nodes[k].port, var.master.port))
  ]
  master_peers = join(",", local.master_peer_list)

  # ---- first filer (default s3 target + cluster env) ----
  filer_keys  = sort(keys(var.filer.nodes))
  first_filer = length(local.filer_keys) > 0 ? format("%s:%d", var.filer.nodes[local.filer_keys[0]].address, coalesce(var.filer.nodes[local.filer_keys[0]].port, var.filer.port)) : ""

  # ---- cluster discovery env (WEED_CLUSTER_*) ----
  cluster_env = merge(
    { WEED_CLUSTER_DEFAULT = var.cluster_name },
    local.master_peers != "" ? { "WEED_CLUSTER_${upper(var.cluster_name)}_MASTER" = local.master_peers } : {},
    local.first_filer != "" ? { "WEED_CLUSTER_${upper(var.cluster_name)}_FILER" = local.first_filer } : {},
  )

  metrics_flags_master = var.monitoring_enabled ? compact([
    format("-metricsPort=%d", var.master.metrics_port),
    var.master.metrics_ip != "" ? format("-metricsIp=%s", var.master.metrics_ip) : "",
  ]) : []
  metrics_flags_volume = var.monitoring_enabled ? compact([
    format("-metricsPort=%d", var.volume.metrics_port),
    var.volume.metrics_ip != "" ? format("-metricsIp=%s", var.volume.metrics_ip) : "",
  ]) : []
  metrics_flags_filer = var.monitoring_enabled ? compact([
    format("-metricsPort=%d", var.filer.metrics_port),
    var.filer.metrics_ip != "" ? format("-metricsIp=%s", var.filer.metrics_ip) : "",
  ]) : []
  metrics_flags_s3 = var.monitoring_enabled ? [format("-metricsPort=%d", var.s3.metrics_port)] : []

  # ===========================================================================
  # MASTER nodes
  # ===========================================================================
  master_nodes = var.master.enabled ? {
    for k in local.master_keys : "master-${k}" => {
      role        = "master"
      name        = "master-${k}"
      address     = var.master.nodes[k].address
      disk_mounts = var.master.nodes[k].disk_mounts
      data_dir    = coalesce(var.master.nodes[k].data_dir, var.master.data_dir)
      data_dirs   = [coalesce(var.master.nodes[k].data_dir, var.master.data_dir)]
      ports = {
        http    = coalesce(var.master.nodes[k].port, var.master.port)
        metrics = var.master.metrics_port
      }
      env          = local.cluster_env
      config_files = local.security_files
      argv = concat(
        local.global_flags,
        ["master"],
        [
          format("-port=%d", coalesce(var.master.nodes[k].port, var.master.port)),
          format("-mdir=%s", coalesce(var.master.nodes[k].data_dir, var.master.data_dir)),
          format("-ip=%s", var.master.nodes[k].address),
          format("-ip.bind=%s", var.master.ip_bind),
          format("-peers=%s", local.master_peers),
          format("-defaultReplication=%s", var.master.default_replication),
          format("-volumeSizeLimitMB=%d", var.master.volume_size_limit_mb),
          format("-electionTimeout=%s", var.master.election_timeout),
          format("-heartbeatInterval=%s", var.master.heartbeat_interval),
        ],
        var.master.nodes[k].grpc_port != null ? [format("-port.grpc=%d", var.master.nodes[k].grpc_port)] : (
        var.master.grpc_port != null ? [format("-port.grpc=%d", var.master.grpc_port)] : []),
        var.master.volume_preallocate ? ["-volumePreallocate"] : [],
        var.master.garbage_threshold != null ? [format("-garbageThreshold=%s", tostring(var.master.garbage_threshold))] : [],
        var.master.raft_hashicorp ? ["-raftHashicorp"] : [],
        var.master.resume_state ? ["-resumeState"] : [],
        var.master.disable_http ? ["-disableHttp"] : [],
        var.master.white_list != "" ? [format("-whiteList=%s", var.master.white_list)] : [],
        local.metrics_flags_master,
        var.master.extra_args,
      )
    }
  } : {}

  # ===========================================================================
  # VOLUME nodes
  # ===========================================================================
  volume_keys = sort(keys(var.volume.nodes))
  volume_nodes = var.volume.enabled ? {
    for k in local.volume_keys : "volume-${k}" => {
      role        = "volume"
      name        = "volume-${k}"
      address     = var.volume.nodes[k].address
      disk_mounts = var.volume.nodes[k].disk_mounts
      data_dirs   = [for d in(var.volume.nodes[k].data_dirs != null ? var.volume.nodes[k].data_dirs : var.volume.data_dirs) : d.path]
      data_dir    = (var.volume.nodes[k].data_dirs != null ? var.volume.nodes[k].data_dirs : var.volume.data_dirs)[0].path
      ports = {
        http    = coalesce(var.volume.nodes[k].port, var.volume.port)
        metrics = var.volume.metrics_port
      }
      env          = local.cluster_env
      config_files = local.security_files
      argv = concat(
        local.global_flags,
        ["volume"],
        [
          format("-port=%d", coalesce(var.volume.nodes[k].port, var.volume.port)),
          format("-dir=%s", join(",", [for d in(var.volume.nodes[k].data_dirs != null ? var.volume.nodes[k].data_dirs : var.volume.data_dirs) : d.path])),
          format("-max=%s", join(",", [for d in(var.volume.nodes[k].data_dirs != null ? var.volume.nodes[k].data_dirs : var.volume.data_dirs) : tostring(d.max_volumes)])),
          format("-ip=%s", var.volume.nodes[k].address),
          format("-ip.bind=%s", var.volume.ip_bind),
          format("-mserver=%s", local.master_peers),
          format("-readMode=%s", var.volume.read_mode),
          format("-compactionMBps=%d", var.volume.compaction_mbps),
          format("-minFreeSpacePercent=%s", var.volume.min_free_space_percent),
          format("-index=%s", var.volume.index),
        ],
        var.volume.nodes[k].grpc_port != null ? [format("-port.grpc=%d", var.volume.nodes[k].grpc_port)] : [],
        (var.volume.nodes[k].idx_dir != null && var.volume.nodes[k].idx_dir != "") ? [format("-dir.idx=%s", var.volume.nodes[k].idx_dir)] : (var.volume.idx_dir != "" ? [format("-dir.idx=%s", var.volume.idx_dir)] : []),
        var.volume.nodes[k].data_center != null ? [format("-dataCenter=%s", var.volume.nodes[k].data_center)] : [],
        var.volume.nodes[k].rack != null ? [format("-rack=%s", var.volume.nodes[k].rack)] : [],
        var.volume.nodes[k].public_url != null ? [format("-publicUrl=%s", var.volume.nodes[k].public_url)] : [],
        var.volume.file_size_limit_mb != null ? [format("-fileSizeLimitMB=%d", var.volume.file_size_limit_mb)] : [],
        var.volume.images_fix_orientation ? ["-images.fix.orientation"] : [],
        var.volume.white_list != "" ? [format("-whiteList=%s", var.volume.white_list)] : [],
        local.metrics_flags_volume,
        var.volume.extra_args,
      )
    }
  } : {}

  # ===========================================================================
  # FILER nodes (+ optional embedded S3)
  # ===========================================================================
  filer_s3_files = var.filer.s3.enabled ? { (var.filer.s3.config_path) = local.s3_config_json } : {}
  filer_nodes = var.filer.enabled ? {
    for k in local.filer_keys : "filer-${k}" => {
      role        = "filer"
      name        = "filer-${k}"
      address     = var.filer.nodes[k].address
      disk_mounts = var.filer.nodes[k].disk_mounts
      data_dir    = coalesce(var.filer.nodes[k].data_dir, var.filer.data_dir)
      data_dirs   = [coalesce(var.filer.nodes[k].data_dir, var.filer.data_dir)]
      ports = {
        http    = coalesce(var.filer.nodes[k].port, var.filer.port)
        metrics = var.filer.metrics_port
      }
      env          = merge(local.cluster_env, var.filer.extra_env)
      config_files = merge(local.security_files, local.filer_s3_files)
      argv = concat(
        local.global_flags,
        ["filer"],
        [
          format("-port=%d", coalesce(var.filer.nodes[k].port, var.filer.port)),
          format("-ip=%s", var.filer.nodes[k].address),
          format("-ip.bind=%s", var.filer.ip_bind),
          format("-master=%s", local.master_peers),
          format("-defaultReplicaPlacement=%s", var.filer.default_replica_placement),
          format("-dirListLimit=%d", var.filer.dir_list_limit),
          format("-defaultStoreDir=%s", coalesce(var.filer.nodes[k].data_dir, var.filer.data_dir)),
        ],
        var.filer.nodes[k].grpc_port != null ? [format("-port.grpc=%d", var.filer.nodes[k].grpc_port)] : [],
        var.filer.max_mb != null ? [format("-maxMB=%d", var.filer.max_mb)] : [],
        var.filer.disable_dir_listing ? ["-disableDirListing"] : [],
        var.filer.disable_http ? ["-disableHttp"] : [],
        var.filer.encrypt_volume_data ? ["-encryptVolumeData"] : [],
        var.filer.rack != "" ? [format("-rack=%s", var.filer.rack)] : [],
        var.filer.data_center != "" ? [format("-dataCenter=%s", var.filer.data_center)] : [],
        var.filer.filer_group != "" ? [format("-filerGroup=%s", var.filer.filer_group)] : [],
        var.filer.s3.enabled ? [
          "-s3",
          format("-s3.port=%d", var.filer.s3.port),
          format("-s3.config=%s", var.filer.s3.config_path),
        ] : [],
        (var.filer.s3.enabled && var.filer.s3.https_port > 0) ? [format("-s3.port.https=%d", var.filer.s3.https_port)] : [],
        (var.filer.s3.enabled && var.filer.s3.domain_name != "") ? [format("-s3.domainName=%s", var.filer.s3.domain_name)] : [],
        local.metrics_flags_filer,
        var.filer.extra_args,
      )
    }
  } : {}

  # ===========================================================================
  # S3 standalone nodes
  # ===========================================================================
  s3_keys      = sort(keys(var.s3.nodes))
  s3_target    = var.s3.filer_address != "" ? var.s3.filer_address : local.first_filer
  s3_std_files = merge(local.security_files, { (var.s3.config_path) = local.s3_config_json })
  s3_nodes = var.s3.enabled ? {
    for k in local.s3_keys : "s3-${k}" => {
      role        = "s3"
      name        = "s3-${k}"
      address     = var.s3.nodes[k].address
      disk_mounts = []
      data_dir    = ""
      data_dirs   = []
      ports = {
        http    = coalesce(var.s3.nodes[k].port, var.s3.port)
        metrics = var.s3.metrics_port
      }
      env          = local.cluster_env
      config_files = local.s3_std_files
      argv = concat(
        local.global_flags,
        ["s3"],
        [
          format("-port=%d", coalesce(var.s3.nodes[k].port, var.s3.port)),
          format("-ip.bind=%s", var.s3.ip_bind),
          format("-filer=%s", local.s3_target),
          format("-config=%s", var.s3.config_path),
        ],
        var.s3.https_port > 0 ? [format("-port.https=%d", var.s3.https_port)] : [],
        var.s3.iceberg_port != null ? [format("-port.iceberg=%d", var.s3.iceberg_port)] : [],
        var.s3.domain_name != "" ? [format("-domainName=%s", var.s3.domain_name)] : [],
        var.s3.audit_log_config_path != "" ? [format("-auditLogConfig=%s", var.s3.audit_log_config_path)] : [],
        var.s3.cert_file != "" ? [format("-cert.file=%s", var.s3.cert_file)] : [],
        var.s3.key_file != "" ? [format("-key.file=%s", var.s3.key_file)] : [],
        var.s3.cacert_file != "" ? [format("-cacert.file=%s", var.s3.cacert_file)] : [],
        var.s3.verify_client_cert ? ["-tlsVerifyClientCert"] : [],
        local.metrics_flags_s3,
        var.s3.extra_args,
      )
    }
  } : {}

  # ===========================================================================
  # ALL-IN-ONE nodes (weed server)
  # ===========================================================================
  aio_keys     = sort(keys(var.all_in_one.nodes))
  aio_s3_files = var.all_in_one.s3.enabled ? { (var.all_in_one.s3.config_path) = local.s3_config_json } : {}
  aio_nodes = var.all_in_one.enabled ? {
    for k in local.aio_keys : "all-in-one-${k}" => {
      role        = "server"
      name        = "all-in-one-${k}"
      address     = var.all_in_one.nodes[k].address
      disk_mounts = var.all_in_one.nodes[k].disk_mounts
      data_dir    = coalesce(var.all_in_one.nodes[k].data_dir, var.all_in_one.data_dir)
      data_dirs   = [coalesce(var.all_in_one.nodes[k].data_dir, var.all_in_one.data_dir)]
      ports = {
        http    = var.all_in_one.master_port
        metrics = var.all_in_one.metrics_port
      }
      env          = local.cluster_env
      config_files = merge(local.security_files, local.aio_s3_files)
      argv = concat(
        local.global_flags,
        ["server"],
        [
          format("-dir=%s", coalesce(var.all_in_one.nodes[k].data_dir, var.all_in_one.data_dir)),
          format("-ip=%s", var.all_in_one.nodes[k].address),
          format("-ip.bind=%s", var.all_in_one.ip_bind),
          "-master",
          format("-master.port=%d", var.all_in_one.master_port),
          format("-master.peers=%s:%d", var.all_in_one.nodes[k].address, var.all_in_one.master_port),
          format("-master.defaultReplication=%s", var.all_in_one.default_replication),
          format("-master.volumeSizeLimitMB=%d", var.all_in_one.volume_size_limit_mb),
          "-volume",
          format("-volume.port=%d", var.all_in_one.volume_port),
          "-filer",
          format("-filer.port=%d", var.all_in_one.filer_port),
          format("-idleTimeout=%d", var.all_in_one.idle_timeout),
        ],
        var.all_in_one.disable_http ? [format("-disableHttp=%t", var.all_in_one.disable_http)] : [],
        var.all_in_one.s3.enabled ? [
          "-s3",
          format("-s3.port=%d", var.all_in_one.s3.port),
          format("-s3.config=%s", var.all_in_one.s3.config_path),
        ] : [],
        (var.all_in_one.s3.enabled && var.all_in_one.s3.domain_name != "") ? [format("-s3.domainName=%s", var.all_in_one.s3.domain_name)] : [],
        var.monitoring_enabled ? [format("-metricsPort=%d", var.all_in_one.metrics_port)] : [],
        var.all_in_one.extra_args,
      )
    }
  } : {}

  # ===========================================================================
  # Merge + render systemd unit and cloud-init per node
  # ===========================================================================
  nodes_base = merge(local.master_nodes, local.volume_nodes, local.filer_nodes, local.s3_nodes, local.aio_nodes)

  # Disk-mount script per node (empty unless the node declares disk_mounts).
  mount_scripts = {
    for name, n in local.nodes_base : name => length(n.disk_mounts) > 0 ? templatefile("${path.module}/templates/mount-disks.sh.tftpl", {
      run_as_user = var.hardening.run_as_user
      mounts      = n.disk_mounts
    }) : ""
  }

  nodes = {
    for name, n in local.nodes_base : name => merge(n, {
      exec_start = "${var.weed_binary} ${join(" ", n.argv)}"
      # secret_files: the secret-bearing config a wrapper should deliver from a
      # secret store when render_secret_files=false (else they go in cloud-init).
      secret_files = n.config_files
      mount_script = local.mount_scripts[name]
      file_modes   = { for p in keys(n.config_files) : p => "0600" }
      systemd_unit = templatefile("${path.module}/templates/weed.service.tftpl", {
        role                = n.role
        name                = n.name
        run_as_user         = var.hardening.run_as_user
        exec_start          = "${var.weed_binary} ${join(" ", n.argv)}"
        no_new_privileges   = var.hardening.no_new_privileges
        protect_system      = var.hardening.protect_system
        cap_drop_all        = var.hardening.cap_drop_all
        read_write_paths    = n.data_dirs
        requires_mounts_for = n.data_dirs
        env_file            = var.env_file
        environment         = n.env
      })
      cloud_init = templatefile("${path.module}/templates/cloud-init.yaml.tftpl", {
        role         = n.role
        name         = n.name
        run_as_user  = var.hardening.run_as_user
        config_files = var.render_secret_files ? n.config_files : {}
        file_modes   = { for p in keys(n.config_files) : p => "0600" }
        pre_runcmd   = [for p in n.data_dirs : "install -d -o ${var.hardening.run_as_user} -g ${var.hardening.run_as_user} ${p}"]
        mount_script = local.mount_scripts[name]
        fetch_script = var.boot_fetch_script
        systemd_unit = templatefile("${path.module}/templates/weed.service.tftpl", {
          role                = n.role
          name                = n.name
          run_as_user         = var.hardening.run_as_user
          exec_start          = "${var.weed_binary} ${join(" ", n.argv)}"
          no_new_privileges   = var.hardening.no_new_privileges
          protect_system      = var.hardening.protect_system
          cap_drop_all        = var.hardening.cap_drop_all
          read_write_paths    = n.data_dirs
          requires_mounts_for = n.data_dirs
          env_file            = var.env_file
          environment         = n.env
        })
      })
    })
  }
}
