output "ca_cert_pem" {
  description = "CA certificate PEM."
  value       = tls_self_signed_cert.ca.cert_pem
}

output "certs" {
  description = "Per-component { cert_pem, private_key_pem }."
  value = {
    for c in local.components : c => {
      cert_pem        = tls_locally_signed_cert.component[c].cert_pem
      private_key_pem = tls_private_key.component[c].private_key_pem
    }
  }
  sensitive = true
}

output "jwt" {
  description = "Generated JWT keys (empty strings when generate_jwt = false)."
  value = {
    signing            = var.generate_jwt ? random_password.jwt["signing"].result : ""
    signing_read       = var.generate_jwt ? random_password.jwt["signing_read"].result : ""
    filer_signing      = var.generate_jwt ? random_password.jwt["filer_signing"].result : ""
    filer_signing_read = var.generate_jwt ? random_password.jwt["filer_signing_read"].result : ""
  }
  sensitive = true
}

# Ready to pass straight to the core module's `security` variable.
output "core_security" {
  description = "Object shaped for the core module's `security` input (cert_dir, allowed_wildcard_domain, JWT keys)."
  value = {
    cert_dir                   = var.cert_dir
    allowed_wildcard_domain    = ".${var.internal_domain}"
    allowed_common_names       = ""
    jwt_signing_key            = var.generate_jwt ? random_password.jwt["signing"].result : ""
    jwt_signing_read_key       = var.generate_jwt ? random_password.jwt["signing_read"].result : ""
    jwt_filer_signing_key      = var.generate_jwt ? random_password.jwt["filer_signing"].result : ""
    jwt_filer_signing_read_key = var.generate_jwt ? random_password.jwt["filer_signing_read"].result : ""
  }
  sensitive = true
}

# Non-sensitive manifest (key, on-host path, mode) so a wrapper can for_each
# over SSM/secret-store resources without tripping the sensitive-for_each rule.
output "secret_manifest" {
  description = "List of { key, path, mode } for the CA + component cert/key files (no content)."
  value = concat(
    [{ key = "ca/tls.crt", path = "${var.cert_dir}/ca/tls.crt", mode = "0644" }],
    flatten([for c in local.components : [
      { key = "${c}/tls.crt", path = "${var.cert_dir}/${c}/tls.crt", mode = "0644" },
      { key = "${c}/tls.key", path = "${var.cert_dir}/${c}/tls.key", mode = "0600" },
    ]]),
  )
}

# Sensitive map keyed by the manifest key -> file content.
output "secret_contents" {
  description = "Map of manifest key -> PEM content."
  value = merge(
    { "ca/tls.crt" = tls_self_signed_cert.ca.cert_pem },
    { for c in local.components : "${c}/tls.crt" => tls_locally_signed_cert.component[c].cert_pem },
    { for c in local.components : "${c}/tls.key" => tls_private_key.component[c].private_key_pem },
  )
  sensitive = true
}
