# =============================================================================
# SeaweedFS security material generator (cloud-agnostic).
#
# Generates the CA, per-component mTLS certs with DISTINCT CommonNames, and JWT
# signing keys. Outputs a `core_security` object ready to feed the core module's
# `security` variable, plus the raw PEMs for a wrapper to deliver via a secret
# store. NOTE: TF-generated secrets live in state; prefer Vault PKI for the CA
# in production.
# =============================================================================

variable "internal_domain" {
  description = "Internal domain for component CNs (e.g. master.<domain>). Drives the allowed_wildcard_domain peer-auth check."
  type        = string
  default     = "seaweedfs.internal"
}

variable "ip_sans" {
  description = "IP addresses to include as SANs on every component cert (all node private IPs + 127.0.0.1 is added automatically)."
  type        = list(string)
  default     = []
}

variable "extra_dns_sans" {
  description = "Additional DNS SANs to include on every component cert."
  type        = list(string)
  default     = []
}

variable "key_algorithm" {
  description = "CA/leaf key algorithm: ECDSA (default, modern) or RSA."
  type        = string
  default     = "ECDSA"
  validation {
    condition     = contains(["ECDSA", "RSA"], var.key_algorithm)
    error_message = "key_algorithm must be ECDSA or RSA."
  }
}

variable "ecdsa_curve" {
  description = "ECDSA curve when key_algorithm = ECDSA."
  type        = string
  default     = "P256"
}

variable "rsa_bits" {
  description = "RSA key size when key_algorithm = RSA."
  type        = number
  default     = 3072
}

variable "ca_validity_hours" {
  description = "CA certificate validity (default 10 years)."
  type        = number
  default     = 87600
}

variable "cert_validity_hours" {
  description = "Component certificate validity. Short by default; certs hot-reload so reissue is cheap."
  type        = number
  default     = 72
}

variable "cert_early_renewal_hours" {
  description = "Renew component certs this many hours before expiry."
  type        = number
  default     = 24
}

variable "cert_dir" {
  description = "On-host directory where certs are placed; surfaced in core_security.cert_dir."
  type        = string
  default     = "/usr/local/share/ca-certificates"
}

variable "generate_jwt" {
  description = "Generate JWT signing keys (signing, signing.read, filer_signing, filer_signing.read)."
  type        = bool
  default     = true
}

variable "jwt_length" {
  description = "Length of generated JWT keys (>= 32 hardened)."
  type        = number
  default     = 40
}
