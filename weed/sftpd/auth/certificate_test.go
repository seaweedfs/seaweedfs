package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"golang.org/x/crypto/ssh"
)

// stubStore is a minimal user.Store for auth tests.
type stubStore struct {
	users map[string]*user.User
}

func newStubStore(usernames ...string) *stubStore {
	s := &stubStore{users: map[string]*user.User{}}
	for _, name := range usernames {
		s.users[name] = &user.User{Username: name}
	}
	return s
}

func (s *stubStore) GetUser(username string) (*user.User, error) {
	if u, ok := s.users[username]; ok {
		return u, nil
	}
	return nil, &user.UserNotFoundError{Username: username}
}
func (s *stubStore) ValidatePassword(string, []byte) bool       { return false }
func (s *stubStore) ValidatePublicKey(string, string) bool      { return false }
func (s *stubStore) GetUserPermissions(string, string) []string { return nil }
func (s *stubStore) SaveUser(*user.User) error                  { return nil }
func (s *stubStore) DeleteUser(string) error                    { return nil }
func (s *stubStore) ListUsers() ([]string, error)               { return nil, nil }

// fakeConnMetadata satisfies ssh.ConnMetadata for the parts CertChecker uses.
type fakeConnMetadata struct {
	user string
}

func (c *fakeConnMetadata) User() string          { return c.user }
func (c *fakeConnMetadata) SessionID() []byte     { return []byte("session") }
func (c *fakeConnMetadata) ClientVersion() []byte { return []byte("SSH-2.0-test") }
func (c *fakeConnMetadata) ServerVersion() []byte { return []byte("SSH-2.0-test") }
func (c *fakeConnMetadata) RemoteAddr() net.Addr  { return &fakeAddr{} }
func (c *fakeConnMetadata) LocalAddr() net.Addr   { return &fakeAddr{} }

type fakeAddr struct{}

func (fakeAddr) Network() string { return "tcp" }
func (fakeAddr) String() string  { return "127.0.0.1:0" }

// testEnv bundles a CA, a user signer, and a temp dir for a single test.
type testEnv struct {
	caSigner   ssh.Signer
	userSigner ssh.Signer
	caKeyFile  string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	_, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519 CA: %v", err)
	}
	caSigner, err := ssh.NewSignerFromKey(caPriv)
	if err != nil {
		t.Fatalf("ca signer: %v", err)
	}

	_, userPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519 user: %v", err)
	}
	userSigner, err := ssh.NewSignerFromKey(userPriv)
	if err != nil {
		t.Fatalf("user signer: %v", err)
	}

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca_user.pub")
	if err := os.WriteFile(caFile, ssh.MarshalAuthorizedKey(caSigner.PublicKey()), 0o600); err != nil {
		t.Fatalf("write ca file: %v", err)
	}

	return &testEnv{caSigner: caSigner, userSigner: userSigner, caKeyFile: caFile}
}

// signCert produces a user cert signed by the test CA.
func (e *testEnv) signCert(t *testing.T, certType uint32, principals []string, validAfter, validBefore time.Time) *ssh.Certificate {
	t.Helper()
	cert := &ssh.Certificate{
		Key:             e.userSigner.PublicKey(),
		CertType:        certType,
		ValidPrincipals: principals,
		ValidAfter:      uint64(validAfter.Unix()),
		ValidBefore:     uint64(validBefore.Unix()),
	}
	if err := cert.SignCert(rand.Reader, e.caSigner); err != nil {
		t.Fatalf("sign cert: %v", err)
	}
	return cert
}

func TestCertificateAuthenticator_GoldenPath(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice")

	a, err := NewCertificateAuthenticator(store, true, env.caKeyFile)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	cert := env.signCert(t, ssh.UserCert, []string{"alice"},
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour))

	perms, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if perms.Extensions["username"] != "alice" {
		t.Fatalf("expected username extension alice, got %q", perms.Extensions["username"])
	}
}

func TestCertificateAuthenticator_RejectsPlainPublicKey(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice")

	a, err := NewCertificateAuthenticator(store, true, env.caKeyFile)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	_, err = a.Authenticate(&fakeConnMetadata{user: "alice"}, env.userSigner.PublicKey())
	if err == nil {
		t.Fatal("expected rejection of plain public key, got nil")
	}
	if !strings.Contains(err.Error(), "public key without certificate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCertificateAuthenticator_RejectsHostCert(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice")
	a, _ := NewCertificateAuthenticator(store, true, env.caKeyFile)

	cert := env.signCert(t, ssh.HostCert, []string{"alice"},
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour))

	if _, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert); err == nil {
		t.Fatal("expected rejection of host cert, got nil")
	}
}

func TestCertificateAuthenticator_RejectsEmptyPrincipals(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice")
	a, _ := NewCertificateAuthenticator(store, true, env.caKeyFile)

	cert := env.signCert(t, ssh.UserCert, nil,
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour))

	_, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert)
	if err == nil || !strings.Contains(err.Error(), "no valid principals") {
		t.Fatalf("expected empty-principals rejection, got %v", err)
	}
}

func TestCertificateAuthenticator_RejectsWrongPrincipal(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice", "bob")
	a, _ := NewCertificateAuthenticator(store, true, env.caKeyFile)

	cert := env.signCert(t, ssh.UserCert, []string{"bob"},
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour))

	if _, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert); err == nil {
		t.Fatal("expected wrong-principal rejection, got nil")
	}
}

func TestCertificateAuthenticator_RejectsExpiredCert(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice")
	a, _ := NewCertificateAuthenticator(store, true, env.caKeyFile)

	cert := env.signCert(t, ssh.UserCert, []string{"alice"},
		time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))

	if _, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert); err == nil {
		t.Fatal("expected expired-cert rejection, got nil")
	}
}

func TestCertificateAuthenticator_RejectsUnknownCA(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore("alice")
	a, _ := NewCertificateAuthenticator(store, true, env.caKeyFile)

	// Sign with a different CA.
	_, otherCAPriv, _ := ed25519.GenerateKey(rand.Reader)
	otherCASigner, _ := ssh.NewSignerFromKey(otherCAPriv)

	cert := &ssh.Certificate{
		Key:             env.userSigner.PublicKey(),
		CertType:        ssh.UserCert,
		ValidPrincipals: []string{"alice"},
		ValidAfter:      uint64(time.Now().Add(-time.Minute).Unix()),
		ValidBefore:     uint64(time.Now().Add(time.Hour).Unix()),
	}
	if err := cert.SignCert(rand.Reader, otherCASigner); err != nil {
		t.Fatalf("sign: %v", err)
	}

	if _, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert); err == nil {
		t.Fatal("expected unknown-CA rejection, got nil")
	}
}

func TestCertificateAuthenticator_RejectsUnknownUser(t *testing.T) {
	env := newTestEnv(t)
	store := newStubStore() // no users
	a, _ := NewCertificateAuthenticator(store, true, env.caKeyFile)

	cert := env.signCert(t, ssh.UserCert, []string{"alice"},
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour))

	_, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected user-not-found, got %v", err)
	}
}

func TestCertificateAuthenticator_LoadsMultipleCAKeys(t *testing.T) {
	env := newTestEnv(t)

	// Append a second CA pubkey to the file.
	_, ca2Priv, _ := ed25519.GenerateKey(rand.Reader)
	ca2Signer, _ := ssh.NewSignerFromKey(ca2Priv)
	f, err := os.OpenFile(env.caKeyFile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Write(ssh.MarshalAuthorizedKey(ca2Signer.PublicKey())); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()

	store := newStubStore("alice")
	a, err := NewCertificateAuthenticator(store, true, env.caKeyFile)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if len(a.trustedCAs) != 2 {
		t.Fatalf("expected 2 CAs, got %d", len(a.trustedCAs))
	}

	// A cert signed by the second CA should be accepted.
	cert := &ssh.Certificate{
		Key:             env.userSigner.PublicKey(),
		CertType:        ssh.UserCert,
		ValidPrincipals: []string{"alice"},
		ValidAfter:      uint64(time.Now().Add(-time.Minute).Unix()),
		ValidBefore:     uint64(time.Now().Add(time.Hour).Unix()),
	}
	if err := cert.SignCert(rand.Reader, ca2Signer); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, cert); err != nil {
		t.Fatalf("Authenticate with second CA: %v", err)
	}
}

func TestNewCertificateAuthenticator_DisabledIgnoresFile(t *testing.T) {
	a, err := NewCertificateAuthenticator(newStubStore(), false, "")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if a.Enabled() {
		t.Fatal("expected disabled")
	}
}

func TestNewCertificateAuthenticator_EnabledRequiresFile(t *testing.T) {
	_, err := NewCertificateAuthenticator(newStubStore(), true, "")
	if err == nil {
		t.Fatal("expected error when enabled without file")
	}
}

func TestNewCertificateAuthenticator_MissingFile(t *testing.T) {
	_, err := NewCertificateAuthenticator(newStubStore(), true, filepath.Join(t.TempDir(), "nope.pub"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	// Sanity: not a not-found-user-style error.
	var notFound *user.UserNotFoundError
	if errors.As(err, &notFound) {
		t.Fatalf("unexpected error type: %v", err)
	}
}
