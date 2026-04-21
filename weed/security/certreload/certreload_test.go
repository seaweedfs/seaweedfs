package certreload

import (
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

	// Rotate the files. pemfile watches mtime; sleep so the new files have
	// a later mtime even on filesystems with 1s granularity.
	time.Sleep(1100 * time.Millisecond)
	if err := writeSelfSigned(certPath, keyPath, "second"); err != nil {
		t.Fatalf("write second cert: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c2, err := getCert(nil)
		if err != nil {
			t.Fatalf("getCert after rotation: %v", err)
		}
		parsed, err := x509.ParseCertificate(c2.Certificate[0])
		if err != nil {
			t.Fatalf("parse rotated cert: %v", err)
		}
		if parsed.Subject.CommonName == "second" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reloader did not pick up rotated cert within timeout")
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
