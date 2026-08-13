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
		DNSNames:     []string{"localhost"},
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

	v := &util.ViperProxy{Viper: viper.New()}
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

	v := &util.ViperProxy{Viper: viper.New()}
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

	v := &util.ViperProxy{Viper: viper.New()}
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)

	addr := startTestGrpcServer(t, v, "grpc.master")
	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.master")); err == nil {
		t.Fatal("expected handshake failure when presenting a serverAuth-only cert as client identity")
	}
}
