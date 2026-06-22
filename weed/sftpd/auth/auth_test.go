package auth

import (
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// When both "publickey" and "certificate" are enabled, certificate auth takes
// over the public-key channel: plain keys are rejected, certs are accepted, and
// password auth stays independently wired.
func TestManager_CertificateTakesOverPublicKeyChannel(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice")

	mgr, err := NewManager(store, []string{"password", "publickey", "certificate"}, env.caKeyFile)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	cfg := mgr.GetSSHServerConfig()
	if cfg.PasswordCallback == nil {
		t.Fatal("expected password callback to stay wired")
	}
	if cfg.PublicKeyCallback == nil {
		t.Fatal("expected public-key callback")
	}

	// Plain public key is rejected by the certificate authenticator.
	if _, err := cfg.PublicKeyCallback(&fakeConnMetadata{user: "alice"}, env.userSigner.PublicKey()); err == nil {
		t.Fatal("expected plain public key to be rejected")
	} else if !strings.Contains(err.Error(), "public key without certificate") {
		t.Fatalf("expected certificate-channel rejection, got %v", err)
	}

	// A valid cert is accepted.
	cert := env.signCert(t, ssh.UserCert, []string{"alice"},
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour))
	perms, err := cfg.PublicKeyCallback(&fakeConnMetadata{user: "alice"}, cert)
	if err != nil {
		t.Fatalf("expected cert to be accepted: %v", err)
	}
	if perms.Extensions["username"] != "alice" {
		t.Fatalf("expected username extension alice, got %q", perms.Extensions["username"])
	}
}

// Without "certificate", the public-key channel keeps plain public-key auth.
func TestManager_PublicKeyOnlyChannel(t *testing.T) {
	mgr, err := NewManager(newStubStore("alice"), []string{"publickey"}, "")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	cfg := mgr.GetSSHServerConfig()
	if cfg.PublicKeyCallback == nil {
		t.Fatal("expected public-key callback")
	}

	env := newTestEnv(t)
	// Routes to plain public-key auth (stub store has no registered key), which
	// fails with a different error than the certificate channel would.
	_, err = cfg.PublicKeyCallback(&fakeConnMetadata{user: "alice"}, env.userSigner.PublicKey())
	if err == nil {
		t.Fatal("expected authentication failure")
	}
	if strings.Contains(err.Error(), "without certificate") {
		t.Fatalf("certificate auth should not be wired, got %v", err)
	}
}

// Enabling certificate auth without a CA keys file is a hard configuration error.
func TestManager_CertificateRequiresCAFile(t *testing.T) {
	if _, err := NewManager(newStubStore(), []string{"certificate"}, ""); err == nil {
		t.Fatal("expected error when certificate auth enabled without CA keys file")
	}
}
