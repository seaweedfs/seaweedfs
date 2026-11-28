package certreload

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/tls/certprovider"
)

// TestServerGetCertificatePicksUpNewFile writes an initial cert/key
// pair, starts the reloader, then overwrites the files with a new pair
// and asserts that the callback eventually returns the new certificate.
func TestServerGetCertificatePicksUpNewFile(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")

	if err := writeSelfSigned(certPath, keyPath, "first"); err != nil {
		t.Fatalf("write first cert: %v", err)
	}

	// Use a short refresh window so the test doesn't wait 5h.
	getCert, provider, err := newServerGetCertificate(certPath, keyPath, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("newServerGetCertificate: %v", err)
	}
	defer provider.Close()

	c1, err := getCert(nil)
	if err != nil {
		t.Fatalf("initial getCert: %v", err)
	}
	first, err := x509.ParseCertificate(c1.Certificate[0])
	if err != nil {
		t.Fatalf("parse first cert: %v", err)
	}
	if first.Subject.CommonName != "first" {
		t.Fatalf("want CN=first, got %q", first.Subject.CommonName)
	}

	// Rotate the files; the periodic refresh should pick up the new pair.
	if err := writeSelfSigned(certPath, keyPath, "second"); err != nil {
		t.Fatalf("write second cert: %v", err)
	}

	// The refresh can briefly observe a mismatched cert/key pair during
	// rotation (cert written before key). Tolerate transient errors and
	// only fail at the deadline; keep the last error for diagnostics.
	var lastErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c2, err := getCert(nil)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		parsed, err := x509.ParseCertificate(c2.Certificate[0])
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if parsed.Subject.CommonName == "second" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reloader did not pick up rotated cert within timeout; last error: %v", lastErr)
}

// TestReloadAllPicksUpNewFileImmediately rotates the files and forces a
// reload, asserting that the new material is served right away instead of
// waiting for the periodic tick.
func TestReloadAllPicksUpNewFileImmediately(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")

	if err := writeSelfSigned(certPath, keyPath, "first"); err != nil {
		t.Fatalf("write first cert: %v", err)
	}

	provider, err := NewProvider(Options{
		CertFile:        certPath,
		KeyFile:         keyPath,
		RefreshDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	defer provider.Close()

	km, err := provider.KeyMaterial(context.Background())
	if err != nil {
		t.Fatalf("initial KeyMaterial: %v", err)
	}
	if got := leafCommonName(t, km); got != "first" {
		t.Fatalf("want CN=first, got %q", got)
	}

	if err := writeSelfSigned(certPath, keyPath, "second"); err != nil {
		t.Fatalf("write second cert: %v", err)
	}
	ReloadAll()

	km, err = provider.KeyMaterial(context.Background())
	if err != nil {
		t.Fatalf("KeyMaterial after reload: %v", err)
	}
	if got := leafCommonName(t, km); got != "second" {
		t.Fatalf("want CN=second after reload, got %q", got)
	}
}

// TestReloadKeepsOldMaterialOnInvalidFile verifies that a broken rotation
// does not take the listener down: Reload reports the error and the last
// good material is still served.
func TestReloadKeepsOldMaterialOnInvalidFile(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")

	if err := writeSelfSigned(certPath, keyPath, "first"); err != nil {
		t.Fatalf("write first cert: %v", err)
	}

	provider, err := NewProvider(Options{
		CertFile:        certPath,
		KeyFile:         keyPath,
		RefreshDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	defer provider.Close()

	good, err := provider.KeyMaterial(context.Background())
	if err != nil {
		t.Fatalf("initial KeyMaterial: %v", err)
	}

	if err := os.WriteFile(certPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("corrupt cert: %v", err)
	}
	if err := provider.Reload(); err == nil {
		t.Fatal("Reload: expected an error for a corrupt certificate")
	}
	ReloadAll()

	km, err := provider.KeyMaterial(context.Background())
	if err != nil {
		t.Fatalf("KeyMaterial after failed reload: %v", err)
	}
	if got := leafCommonName(t, km); got != "first" {
		t.Fatalf("want old CN=first after failed reload, got %q", got)
	}
	if leafCommonName(t, good) != "first" {
		t.Fatal("previous material was mutated by a failed reload")
	}
}

func TestNewProviderValidatesOptions(t *testing.T) {
	if _, err := NewProvider(Options{CertFile: "cert.pem"}); err == nil {
		t.Fatal("expected an error for a cert without a key")
	}
	if _, err := NewProvider(Options{}); err == nil {
		t.Fatal("expected an error for an empty options set")
	}
}

func leafCommonName(t *testing.T, km *certprovider.KeyMaterial) string {
	t.Helper()
	if len(km.Certs) == 0 {
		t.Fatal("key material has no certificates")
	}
	leaf, err := x509.ParseCertificate(km.Certs[0].Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf.Subject.CommonName
}

func writeSelfSigned(certPath, keyPath, commonName string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		certOut.Close()
		return err
	}
	if err := certOut.Close(); err != nil {
		return err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	keyOut, err := os.Create(keyPath)
	if err != nil {
		return err
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		keyOut.Close()
		return err
	}
	return keyOut.Close()
}
