package auth

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"golang.org/x/crypto/ssh"
)

// CertificateAuthenticator authenticates clients that present an OpenSSH
// user certificate signed by one of the configured trusted CA public keys.
//
// Behaviour mirrors MinIO's --sftp=trusted-user-ca-key option and OpenSSH's
// TrustedUserCAKeys directive: when enabled, only key blobs of type
// *ssh.Certificate are accepted on the public-key channel. Plain public
// keys are rejected. The SSH login username must appear in the cert's
// ValidPrincipals list and must resolve to an existing user in the store.
type CertificateAuthenticator struct {
	userStore  user.Store
	enabled    bool
	trustedCAs []ssh.PublicKey
	checker    *ssh.CertChecker
}

// NewCertificateAuthenticator constructs a CertificateAuthenticator.
// When enabled is true, caKeysFile must point to a file containing one or
// more CA public keys in OpenSSH authorized_keys format (one per line).
func NewCertificateAuthenticator(userStore user.Store, enabled bool, caKeysFile string) (*CertificateAuthenticator, error) {
	a := &CertificateAuthenticator{
		userStore: userStore,
		enabled:   enabled,
	}

	if !enabled {
		return a, nil
	}

	if caKeysFile == "" {
		return nil, fmt.Errorf("certificate auth enabled but no trustedUserCAKeysFile provided")
	}

	cas, err := loadAuthorizedKeysFile(caKeysFile)
	if err != nil {
		return nil, fmt.Errorf("load trusted user CA keys from %s: %w", caKeysFile, err)
	}
	if len(cas) == 0 {
		return nil, fmt.Errorf("no trusted user CA keys found in %s", caKeysFile)
	}
	a.trustedCAs = cas

	a.checker = &ssh.CertChecker{
		IsUserAuthority: func(auth ssh.PublicKey) bool {
			marshaled := auth.Marshal()
			for _, ca := range a.trustedCAs {
				if subtle.ConstantTimeCompare(marshaled, ca.Marshal()) == 1 {
					return true
				}
			}
			return false
		},
	}

	return a, nil
}

// Enabled reports whether certificate authentication is active.
func (a *CertificateAuthenticator) Enabled() bool {
	return a.enabled
}

// Authenticate implements ssh.ServerConfig.PublicKeyCallback.
func (a *CertificateAuthenticator) Authenticate(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	if !a.enabled {
		return nil, fmt.Errorf("certificate authentication disabled")
	}

	cert, ok := key.(*ssh.Certificate)
	if !ok {
		return nil, fmt.Errorf("public key without certificate not allowed")
	}
	if cert.CertType != ssh.UserCert {
		return nil, fmt.Errorf("certificate is not a user certificate")
	}
	if len(cert.ValidPrincipals) == 0 {
		return nil, fmt.Errorf("certificate has no valid principals")
	}

	username := conn.User()

	// CertChecker.Authenticate verifies the CA signature (via IsUserAuthority),
	// the ValidAfter/ValidBefore window, and that username is in ValidPrincipals.
	perms, err := a.checker.Authenticate(conn, key)
	if err != nil {
		return nil, fmt.Errorf("certificate validation failed: %w", err)
	}

	// The SSH login user must exist in the SeaweedFS user store.
	if _, err := a.userStore.GetUser(username); err != nil {
		return nil, fmt.Errorf("user %q not found", username)
	}

	if perms == nil {
		perms = &ssh.Permissions{}
	}
	if perms.Extensions == nil {
		perms.Extensions = map[string]string{}
	}
	perms.Extensions["username"] = username
	return perms, nil
}

// loadAuthorizedKeysFile parses an authorized_keys-style file and returns
// all public keys found in it. Blank lines and comment lines starting with
// '#' are skipped.
func loadAuthorizedKeysFile(path string) ([]ssh.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var keys []ssh.PublicKey
	rest := data
	for len(bytes.TrimSpace(rest)) > 0 {
		pub, _, _, next, err := ssh.ParseAuthorizedKey(rest)
		if err != nil {
			return nil, err
		}
		keys = append(keys, pub)
		rest = next
	}
	return keys, nil
}
