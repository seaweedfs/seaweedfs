locals {
  components = ["master", "volume", "filer", "client"]
  ip_sans    = distinct(concat(["127.0.0.1"], var.ip_sans))
}

# ---- CA ---------------------------------------------------------------------
resource "tls_private_key" "ca" {
  algorithm   = var.key_algorithm
  ecdsa_curve = var.ecdsa_curve
  rsa_bits    = var.rsa_bits
}

resource "tls_self_signed_cert" "ca" {
  private_key_pem   = tls_private_key.ca.private_key_pem
  is_ca_certificate = true

  subject {
    common_name  = "SeaweedFS CA"
    organization = "SeaweedFS"
  }

  validity_period_hours = var.ca_validity_hours
  allowed_uses          = ["cert_signing", "crl_signing", "digital_signature"]
}

# ---- per-component leaf certs (distinct CN per component) --------------------
resource "tls_private_key" "component" {
  for_each    = toset(local.components)
  algorithm   = var.key_algorithm
  ecdsa_curve = var.ecdsa_curve
  rsa_bits    = var.rsa_bits
}

resource "tls_cert_request" "component" {
  for_each        = toset(local.components)
  private_key_pem = tls_private_key.component[each.key].private_key_pem

  subject {
    # Distinct CN per component so peer-identity authZ (allowed_wildcard_domain
    # / allowed_commonNames) is meaningful. Do NOT use one CN for all.
    common_name  = "${each.key}.${var.internal_domain}"
    organization = "SeaweedFS"
  }

  dns_names = distinct(concat(
    ["${each.key}.${var.internal_domain}", "localhost"],
    var.extra_dns_sans,
  ))
  ip_addresses = local.ip_sans
}

resource "tls_locally_signed_cert" "component" {
  for_each           = toset(local.components)
  cert_request_pem   = tls_cert_request.component[each.key].cert_request_pem
  ca_private_key_pem = tls_private_key.ca.private_key_pem
  ca_cert_pem        = tls_self_signed_cert.ca.cert_pem

  validity_period_hours = var.cert_validity_hours
  early_renewal_hours   = var.cert_early_renewal_hours
  allowed_uses          = ["digital_signature", "key_agreement", "server_auth", "client_auth"]
}

# ---- JWT signing keys -------------------------------------------------------
resource "random_password" "jwt" {
  for_each = var.generate_jwt ? toset(["signing", "signing_read", "filer_signing", "filer_signing_read"]) : toset([])
  length   = var.jwt_length
  special  = false # TOML-safe
}
