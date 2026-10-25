package auth

import (
	"bytes"
	"crypto/subtle"
	"errors"
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

	// Pre-marshal trusted CA keys once. IsUserAuthority runs on every
	// authentication attempt, so caching the marshaled form avoids
	// repeated allocations on the hot path.
	trustedCAsMarshaled := make([][]byte, len(cas))
	for i, ca := range cas {
		trustedCAsMarshaled[i] = ca.Marshal()
	}

	a.checker = &ssh.CertChecker{
		IsUserAuthority: func(auth ssh.PublicKey) bool {
			marshaled := auth.Marshal()
			for _, caBytes := range trustedCAsMarshaled {
				if subtle.ConstantTimeCompare(marshaled, caBytes) == 1 {
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
		var notFound *user.UserNotFoundError
		if errors.As(err, &notFound) {
			return nil, fmt.Errorf("user %q not found", username)
		}
		return nil, fmt.Errorf("lookup user %q: %w", username, err)
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
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		pub, _, _, _, err := ssh.ParseAuthorizedKey(line)
		if err != nil {
			return nil, err
		}
		keys = append(keys, pub)
	}
	return keys, nil
}
