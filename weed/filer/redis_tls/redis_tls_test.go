package redis_tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeConfiguration map[string]interface{}

func (c fakeConfiguration) GetString(key string) string {
	value, _ := c[key].(string)
	return value
}

func (c fakeConfiguration) GetBool(key string) bool {
	value, _ := c[key].(bool)
	return value
}

func (c fakeConfiguration) GetInt(key string) int {
	value, _ := c[key].(int)
	return value
}

func (c fakeConfiguration) GetStringSlice(key string) []string {
	value, _ := c[key].([]string)
	return value
}

func (c fakeConfiguration) SetDefault(key string, value interface{}) {
	if _, found := c[key]; !found {
		c[key] = value
	}
}

func TestDisabled(t *testing.T) {
	tlsConfig, err := Config(fakeConfiguration{"redis2.ca_cert_path": "/does/not/exist"}, "redis2.")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsConfig != nil {
		t.Fatalf("expected no TLS config when neither enable_tls nor enable_mtls is set")
	}
}

func TestEnabledWithoutCertificates(t *testing.T) {
	for _, key := range []string{"enable_tls", "enable_mtls"} {
		tlsConfig, err := Config(fakeConfiguration{"redis2." + key: true}, "redis2.")
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", key, err)
		}
		if tlsConfig == nil {
			t.Fatalf("%s: expected a TLS config", key)
		}
		if tlsConfig.MinVersion != tls.VersionTLS12 {
			t.Errorf("%s: expected TLS 1.2 minimum, got %x", key, tlsConfig.MinVersion)
		}
		if tlsConfig.RootCAs != nil {
			t.Errorf("%s: expected the system root pool", key)
		}
		if len(tlsConfig.Certificates) != 0 {
			t.Errorf("%s: expected no client certificate", key)
		}
		if tlsConfig.ServerName != "" {
			t.Errorf("%s: expected the server name to come from the dialed address, got %s", key, tlsConfig.ServerName)
		}
	}
}

func TestMutualTls(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertificate(t, dir, "client")

	tlsConfig, err := Config(fakeConfiguration{
		"redis2.enable_tls":       true,
		"redis2.ca_cert_path":     certPath,
		"redis2.client_cert_path": certPath,
		"redis2.client_key_path":  keyPath,
	}, "redis2.")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsConfig.RootCAs == nil {
		t.Errorf("expected the CA certificate to be loaded")
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("expected the client certificate to be loaded, got %d", len(tlsConfig.Certificates))
	}
}

func TestUnreadableCaCertificate(t *testing.T) {
	if _, err := Config(fakeConfiguration{
		"redis2.enable_tls":   true,
		"redis2.ca_cert_path": filepath.Join(t.TempDir(), "missing.pem"),
	}, "redis2."); err == nil {
		t.Fatalf("expected an error for a missing CA certificate")
	}
}

func TestCaCertificateWithoutPem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Config(fakeConfiguration{
		"redis2.enable_tls":   true,
		"redis2.ca_cert_path": path,
	}, "redis2."); err == nil {
		t.Fatalf("expected an error for a CA file without any certificate")
	}
}

func TestClientCertificateWithoutKey(t *testing.T) {
	certPath, _ := writeCertificate(t, t.TempDir(), "client")
	if _, err := Config(fakeConfiguration{
		"redis2.enable_tls":       true,
		"redis2.client_cert_path": certPath,
	}, "redis2."); err == nil {
		t.Fatalf("expected an error for a client certificate without a key")
	}
}

func writeCertificate(t *testing.T, dir, name string) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31-1, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}
