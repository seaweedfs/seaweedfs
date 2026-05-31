# ---- step 1: reserve stable addressing (ENIs with fixed private IPs) -------
# These exist BEFORE the core renders config, so the address map is known and
# the core <- wrapper dependency is one-way (no apply-time cycle).

resource "aws_network_interface" "master" {
  for_each        = var.masters
  subnet_id       = each.value.subnet_id
  private_ips     = [each.value.private_ip]
  security_groups = [aws_security_group.cluster.id]
  tags            = merge(var.tags, { Name = "${var.name}-master-${each.key}", Role = "master" })
}

resource "aws_network_interface" "volume" {
  for_each        = var.volumes
  subnet_id       = each.value.subnet_id
  private_ips     = [each.value.private_ip]
  security_groups = [aws_security_group.cluster.id]
  tags            = merge(var.tags, { Name = "${var.name}-volume-${each.key}", Role = "volume" })
}

resource "aws_network_interface" "filer" {
  for_each        = var.filers
  subnet_id       = each.value.subnet_id
  private_ips     = [each.value.private_ip]
  security_groups = [aws_security_group.cluster.id]
  tags            = merge(var.tags, { Name = "${var.name}-filer-${each.key}", Role = "filer" })
}

resource "aws_network_interface" "s3" {
  for_each        = var.s3_nodes
  subnet_id       = each.value.subnet_id
  private_ips     = [each.value.private_ip]
  security_groups = [aws_security_group.cluster.id]
  tags            = merge(var.tags, { Name = "${var.name}-s3-${each.key}", Role = "s3" })
}

# ---- step 2: render config with the cloud-agnostic core --------------------
module "core" {
  source = "../core"

  weed_binary        = var.weed_binary
  monitoring_enabled = var.monitoring_enabled
  enable_security    = var.enable_security
  security           = local.core_security

  # Keep secrets out of cloud-init user_data when security is on; deliver them
  # from SSM via the boot fetch script instead.
  render_secret_files = !var.enable_security
  boot_fetch_script   = local.fetch_script

  master = {
    nodes = { for k, v in var.masters : k => { address = v.private_ip } }
  }

  volume = {
    enabled = length(var.volumes) > 0
    nodes = { for k, v in var.volumes : k => {
      address     = v.private_ip
      rack        = v.rack
      data_center = v.data_center
      data_dirs   = [{ path = "/data", max_volumes = 0 }]
      # auto-discover the single attached data disk and mount it at /data
      disk_mounts = [{ mountpoint = "/data", fstype = "xfs" }]
    } }
  }

  filer = {
    enabled = length(var.filers) > 0
    nodes = { for k, v in var.filers : k => {
      address     = v.private_ip
      data_dir    = "/data/filerldb2"
      disk_mounts = [{ mountpoint = "/data", fstype = "xfs" }]
    } }
    s3 = var.embedded_s3
  }

  s3 = {
    enabled = length(var.s3_nodes) > 0
    nodes   = { for k, v in var.s3_nodes : k => { address = v.private_ip } }
  }

  s3_identities = var.s3_identities
}

# ---- step 3: instances (keyed for_each; user_data = rendered cloud-init) ----
resource "aws_instance" "master" {
  for_each                = var.masters
  ami                     = var.ami_id
  instance_type           = each.value.instance_type
  key_name                = var.key_name
  user_data               = module.core.cloud_init_by_node["master-${each.key}"]
  iam_instance_profile    = var.enable_security ? aws_iam_instance_profile.node[0].name : null
  disable_api_termination = var.termination_protection

  network_interface {
    network_interface_id = aws_network_interface.master[each.key].id
    device_index         = 0
  }
  metadata_options {
    http_tokens = "required" # IMDSv2
  }
  tags = merge(var.tags, { Name = "${var.name}-master-${each.key}", Role = "master" })
}

resource "aws_instance" "volume" {
  for_each                = var.volumes
  ami                     = var.ami_id
  instance_type           = each.value.instance_type
  key_name                = var.key_name
  user_data               = module.core.cloud_init_by_node["volume-${each.key}"]
  iam_instance_profile    = var.enable_security ? aws_iam_instance_profile.node[0].name : null
  disable_api_termination = var.termination_protection

  network_interface {
    network_interface_id = aws_network_interface.volume[each.key].id
    device_index         = 0
  }
  metadata_options {
    http_tokens = "required"
  }
  tags = merge(var.tags, { Name = "${var.name}-volume-${each.key}", Role = "volume" })
}

resource "aws_instance" "filer" {
  for_each             = var.filers
  ami                  = var.ami_id
  instance_type        = each.value.instance_type
  key_name             = var.key_name
  user_data            = module.core.cloud_init_by_node["filer-${each.key}"]
  iam_instance_profile = var.enable_security ? aws_iam_instance_profile.node[0].name : null

  network_interface {
    network_interface_id = aws_network_interface.filer[each.key].id
    device_index         = 0
  }
  metadata_options {
    http_tokens = "required"
  }
  tags = merge(var.tags, { Name = "${var.name}-filer-${each.key}", Role = "filer" })
}

resource "aws_instance" "s3" {
  for_each             = var.s3_nodes
  ami                  = var.ami_id
  instance_type        = each.value.instance_type
  key_name             = var.key_name
  user_data            = module.core.cloud_init_by_node["s3-${each.key}"]
  iam_instance_profile = var.enable_security ? aws_iam_instance_profile.node[0].name : null

  network_interface {
    network_interface_id = aws_network_interface.s3[each.key].id
    device_index         = 0
  }
  metadata_options {
    http_tokens = "required"
  }
  tags = merge(var.tags, { Name = "${var.name}-s3-${each.key}", Role = "s3" })
}

# ---- step 4: protected data disks (decoupled; survive instance replacement) -
# NOTE: cloud-init must mkfs+mount these at /data before the unit starts; that
# mount-disks step is a documented follow-up (see terraform/README.md).
resource "aws_ebs_volume" "volume_data" {
  for_each          = var.volumes
  availability_zone = each.value.availability_zone
  size              = each.value.data_volume_size_gb
  type              = each.value.data_volume_type
  iops              = each.value.data_volume_iops
  tags              = merge(var.tags, { Name = "${var.name}-volume-${each.key}-data", Role = "volume" })

  lifecycle {
    prevent_destroy = true # break-glass: remove to allow destroy (see plan §5)
  }
}

resource "aws_volume_attachment" "volume_data" {
  for_each                       = var.volumes
  device_name                    = "/dev/xvdf"
  volume_id                      = aws_ebs_volume.volume_data[each.key].id
  instance_id                    = aws_instance.volume[each.key].id
  stop_instance_before_detaching = true
}

resource "aws_ebs_volume" "filer_data" {
  for_each          = var.filers
  availability_zone = each.value.availability_zone
  size              = each.value.data_volume_size_gb
  type              = "gp3"
  tags              = merge(var.tags, { Name = "${var.name}-filer-${each.key}-data", Role = "filer" })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_volume_attachment" "filer_data" {
  for_each                       = var.filers
  device_name                    = "/dev/xvdf"
  volume_id                      = aws_ebs_volume.filer_data[each.key].id
  instance_id                    = aws_instance.filer[each.key].id
  stop_instance_before_detaching = true
}
