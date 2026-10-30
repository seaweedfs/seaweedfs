package security

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newTestCA(t *testing.T, dir string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "ca.pem")
	writePem(t, file, "CERTIFICATE", der)
	return &testCA{cert: cert, key: key, file: file}
}

// issue creates a leaf certificate restricted to the given extended key usages.
func (ca *testCA) issue(t *testing.T, dir, name string, ekus []x509.ExtKeyUsage) (certFile, keyFile string) {
	return ca.issueWithSANs(t, dir, name, ekus, []string{"localhost"}, nil)
}

func (ca *testCA) issueWithSANs(t *testing.T, dir, name string, ekus []x509.ExtKeyUsage, dnsNames []string, uris []string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  ekus,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     dnsNames,
	}
	for _, u := range uris {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = append(template.URIs, parsed)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDer, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, name+".pem")
	keyFile = filepath.Join(dir, name+".key")
	writePem(t, certFile, "CERTIFICATE", der)
	writePem(t, keyFile, "EC PRIVATE KEY", keyDer)
	return certFile, keyFile
}

func writePem(t *testing.T, file, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
}

func startTestGrpcServer(t *testing.T, config *util.ViperProxy, component string) string {
	t.Helper()
	creds, _ := LoadServerTLS(config, component)
	if creds == nil {
		t.Fatal("LoadServerTLS returned nil")
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(creds)
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

func healthCheck(t *testing.T, addr string, dialOption grpc.DialOption) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, dialOption)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

// A component configured with a serverAuth-only serving cert plus a
// clientAuth-only client_cert/client_key pair must dial with the client pair.
func TestLoadClientTLSPrefersClientCert(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issue(t, dir, "server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.master.client_cert", clientCert)
	v.Set("grpc.master.client_key", clientKey)

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err != nil {
		t.Fatalf("health check with split client cert failed: %v", err)
	}
}

// Without client_cert, the component keeps presenting its serving cert.
func TestLoadClientTLSFallsBackToServingCert(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	dualCert, dualKey := ca.issue(t, dir, "dual", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", dualCert)
	v.Set("grpc.master.key", dualKey)

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err != nil {
		t.Fatalf("health check with dual-EKU cert failed: %v", err)
	}
}

// A serverAuth-only cert presented as the client identity fails the peer's
// clientAuth EKU verification — the failure mode client_cert exists to fix.
func TestLoadClientTLSServerOnlyEkuRejected(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issue(t, dir, "server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err == nil {
		t.Fatal("expected handshake failure when presenting a serverAuth-only cert as client identity")
	}
}

func TestLoadServerTLSUnconfiguredIsPlaintext(t *testing.T) {
	v := util.NewViperProxy(viper.New())
	creds, _, err := loadServerTLS(v, "grpc.master")
	if err != nil || creds != nil {
		t.Fatalf("expected no TLS and no error, got creds=%v err=%v", creds, err)
	}
}

func TestLoadServerTLSPartialCertKeyFails(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	certFile, _ := ca.issue(t, dir, "server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", certFile)

	if _, _, err := loadServerTLS(v, "grpc.master"); err == nil {
		t.Fatal("expected error when only cert is set")
	}
}

func TestLoadServerTLSBadVersionNameFails(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	certFile, keyFile := ca.issue(t, dir, "server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", certFile)
	v.Set("grpc.master.key", keyFile)
	v.Set("tls.min_version", "bogus")

	if _, _, err := loadServerTLS(v, "grpc.master"); err == nil {
		t.Fatal("expected error for unparseable tls.min_version")
	}
}

func TestLoadClientTLSUnconfiguredIsPlaintext(t *testing.T) {
	v := util.NewViperProxy(viper.New())
	if _, err := loadClientTLS(v, "grpc.client"); err != nil {
		t.Fatalf("expected insecure dial option, got err=%v", err)
	}
}

func TestLoadClientTLSMissingCaFails(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	certFile, keyFile := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.client.cert", certFile)
	v.Set("grpc.client.key", keyFile)

	if _, err := loadClientTLS(v, "grpc.client"); err == nil {
		t.Fatal("expected error when cert and key are set without grpc.ca")
	}
}

func TestLoadClientTLSBadCaPathFails(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	certFile, keyFile := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", filepath.Join(dir, "missing-ca.pem"))
	v.Set("grpc.client.cert", certFile)
	v.Set("grpc.client.key", keyFile)

	if _, err := loadClientTLS(v, "grpc.client"); err == nil {
		t.Fatal("expected error for unreadable grpc.ca")
	}
}

func TestLoadClientTLSAllowsListedServerName(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issue(t, dir, "seaweed-filer", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.master.client_cert", clientCert)
	v.Set("grpc.master.client_key", clientKey)
	v.Set("grpc.master.allowed_server_commonNames", "seaweed-filer,seaweed-volume")

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err != nil {
		t.Fatalf("health check with listed server name failed: %v", err)
	}
}

// Any CA-signed cert must not be able to stand in for another component:
// a filer's identity dialing as "the master" has to fail the allowlist.
func TestLoadClientTLSRejectsUnlistedServerName(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issue(t, dir, "seaweed-filer", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.master.client_cert", clientCert)
	v.Set("grpc.master.client_key", clientKey)
	v.Set("grpc.master.allowed_server_commonNames", "seaweed-master")

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err == nil {
		t.Fatal("expected handshake failure for a server identity outside the allowlist")
	}
}

func TestLoadClientTLSMatchesServerURISAN(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issueWithSANs(t, dir, "server",
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, nil, []string{"spiffe://cluster.local/ns/seaweedfs/master"})
	clientCert, clientKey := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.master.client_cert", clientCert)
	v.Set("grpc.master.client_key", clientKey)
	v.Set("grpc.master.allowed_server_commonNames", "spiffe://cluster.local/ns/seaweedfs/master")

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err != nil {
		t.Fatalf("health check with URI SAN allowlist failed: %v", err)
	}
}

func TestLoadClientTLSMatchesServerWildcardDomain(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issueWithSANs(t, dir, "server",
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, []string{"master.example.com"}, nil)
	clientCert, clientKey := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.master.client_cert", clientCert)
	v.Set("grpc.master.client_key", clientKey)
	v.Set("grpc.allowed_server_wildcard_domain", ".example.com")

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err != nil {
		t.Fatalf("health check with wildcard domain failed: %v", err)
	}
}

func TestLoadClientTLSGlobalServerNamesFallback(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issue(t, dir, "seaweed-filer", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := ca.issue(t, dir, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.master.client_cert", clientCert)
	v.Set("grpc.master.client_key", clientKey)
	v.Set("grpc.allowed_server_commonNames", "seaweed-filer")

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err != nil {
		t.Fatalf("health check with global allowlist failed: %v", err)
	}
}
