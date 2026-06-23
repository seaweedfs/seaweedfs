# =============================================================================
# mTLS + JWT generation and secret delivery (active when enable_security=true).
#
# Generates the CA + per-component certs + JWT keys with the security submodule,
# stores them (plus the rendered security.toml / S3 config) in SSM Parameter
# Store as SecureString, and renders a boot fetch-secrets.sh that pulls them via
# the instance role. This keeps secrets OUT of cloud-init user_data / IMDS.
# Secrets still live in Terraform state (TF-generated); prefer Vault PKI for the
# CA in production.
# =============================================================================

locals {
  all_private_ips = concat(
    [for k, v in var.masters : v.private_ip],
    [for k, v in var.volumes : v.private_ip],
    [for k, v in var.filers : v.private_ip],
    [for k, v in var.s3_nodes : v.private_ip],
  )

  ssm_prefix        = "/seaweedfs/${var.name}"
  deliver_s3_config = length(var.s3_nodes) > 0 || var.embedded_s3.enabled

  # security.toml is identical across nodes; take it from the first master.
  first_master       = var.enable_security ? sort(keys(var.masters))[0] : ""
  security_toml_path = "/etc/seaweedfs/security.toml"
  s3_config_path     = "/etc/seaweedfs/s3_config.json"

  # Boot fetch entries: cert files + security.toml + (optional) S3 config.
  fetch_entries = var.enable_security ? concat(
    [for m in module.security[0].secret_manifest : { param = "${local.ssm_prefix}/certs/${m.key}", path = m.path, mode = m.mode }],
    [{ param = "${local.ssm_prefix}/security.toml", path = local.security_toml_path, mode = "0600" }],
    local.deliver_s3_config ? [{ param = "${local.ssm_prefix}/s3_config.json", path = local.s3_config_path, mode = "0600" }] : [],
  ) : []

  fetch_script = var.enable_security ? templatefile("${path.module}/templates/fetch-secrets.sh.tftpl", {
    run_as_user = "seaweedfs"
    entries     = local.fetch_entries
  }) : ""

  core_security = var.enable_security ? module.security[0].core_security : var.security
}

module "security" {
  count           = var.enable_security ? 1 : 0
  source          = "../security"
  internal_domain = var.internal_domain
  cert_dir        = var.cert_dir
  ip_sans         = local.all_private_ips
}

# ---- SSM SecureString parameters -------------------------------------------
resource "aws_ssm_parameter" "cert" {
  for_each = var.enable_security ? { for m in module.security[0].secret_manifest : m.key => m } : {}
  name     = "${local.ssm_prefix}/certs/${each.key}"
  type     = "SecureString"
  value    = module.security[0].secret_contents[each.key]
  tags     = var.tags
}

resource "aws_ssm_parameter" "security_toml" {
  count = var.enable_security ? 1 : 0
  name  = "${local.ssm_prefix}/security.toml"
  type  = "SecureString"
  value = module.core.secret_files_by_node["master-${local.first_master}"][local.security_toml_path]
  tags  = var.tags
}

resource "aws_ssm_parameter" "s3_config" {
  count = var.enable_security && local.deliver_s3_config ? 1 : 0
  name  = "${local.ssm_prefix}/s3_config.json"
  type  = "SecureString"
  # identical content across s3/filer nodes; render once via the security module's
  # JSON is not available here, so read it back from any node that carries it.
  value = local.s3_config_source
  tags  = var.tags
}

locals {
  # Pull the rendered S3 identity JSON from whichever node carries it.
  s3_config_source = !local.deliver_s3_config ? "" : (
    length(var.s3_nodes) > 0 ?
    module.core.secret_files_by_node["s3-${sort(keys(var.s3_nodes))[0]}"][local.s3_config_path] :
    (length(var.filers) > 0 ? module.core.secret_files_by_node["filer-${sort(keys(var.filers))[0]}"][local.s3_config_path] : "")
  )
}

# ---- IAM: instance role allowed to read the SSM params ----------------------
data "aws_iam_policy_document" "assume" {
  count = var.enable_security ? 1 : 0
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "ssm_read" {
  count = var.enable_security ? 1 : 0
  statement {
    sid       = "ReadSeaweedfsParams"
    actions   = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = ["arn:aws:ssm:*:*:parameter${local.ssm_prefix}/*"]
  }
  statement {
    sid       = "DecryptSecureStrings"
    actions   = ["kms:Decrypt"]
    resources = [var.kms_key_arn] # defaults to "*"; set a CMK ARN in production
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.*.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  count              = var.enable_security ? 1 : 0
  name_prefix        = "${var.name}-node-"
  assume_role_policy = data.aws_iam_policy_document.assume[0].json
  tags               = var.tags
}

resource "aws_iam_role_policy" "ssm_read" {
  count  = var.enable_security ? 1 : 0
  name   = "ssm-read"
  role   = aws_iam_role.node[0].id
  policy = data.aws_iam_policy_document.ssm_read[0].json
}

resource "aws_iam_instance_profile" "node" {
  count       = var.enable_security ? 1 : 0
  name_prefix = "${var.name}-node-"
  role        = aws_iam_role.node[0].name
}
