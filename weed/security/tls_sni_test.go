package security

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/security/advancedtls"
)

func generateSelfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	leafCert := tls.Certificate{
		Certificate: [][]byte{leafDER},
		PrivateKey:  leafKey,
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return leafCert, pool
}

func startTLSListenerCapturingSNI(t *testing.T, cert tls.Certificate) (string, <-chan string) {
	t.Helper()

	sniChan := make(chan string, 1)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			select {
			case sniChan <- hello.ServerName:
				close(sniChan)
			default:
			}
			return nil, nil
		},
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		conn.Read(buf)
	}()

	return ln.Addr().String(), sniChan
}

func newSNIStrippingCreds(t *testing.T, cert tls.Certificate, pool *x509.CertPool) credentials.TransportCredentials {
	t.Helper()
	clientCreds, err := advancedtls.NewClientCreds(&advancedtls.Options{
		IdentityOptions: advancedtls.IdentityCertificateOptions{
			Certificates: []tls.Certificate{cert},
		},
		RootOptions: advancedtls.RootCertificateOptions{
			RootCertificates: pool,
		},
		VerificationType: advancedtls.CertVerification,
		AdditionalPeerVerification: func(params *advancedtls.HandshakeVerificationInfo) (*advancedtls.PostHandshakeVerificationResults, error) {
			return &advancedtls.PostHandshakeVerificationResults{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewClientCreds: %v", err)
	}
	return &SNIStrippingTransportCredentials{creds: clientCreds}
}

func TestSNI_HostnameStripsPort(t *testing.T) {
	cert, pool := generateSelfSignedCert(t)
	wrapped := newSNIStrippingCreds(t, cert, pool)
	addr, sniChan := startTLSListenerCapturingSNI(t, cert)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// gRPC passes "host:port" as authority; SNI wrapper strips the port
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = wrapped.ClientHandshake(ctx, "localhost:"+portFromAddr(addr), conn)
	if err != nil {
		t.Fatalf("ClientHandshake: %v", err)
	}

	select {
	case sni := <-sniChan:
		if sni != "localhost" {
			t.Errorf("SNI = %q, want %q", sni, "localhost")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SNI")
	}
}

func TestSNI_IPAddressEmptySNI(t *testing.T) {
	cert, pool := generateSelfSignedCert(t)
	wrapped := newSNIStrippingCreds(t, cert, pool)
	addr, sniChan := startTLSListenerCapturingSNI(t, cert)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// RFC 6066: IP addresses MUST NOT be sent as SNI; Go's TLS sends empty ServerName for IPs
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = wrapped.ClientHandshake(ctx, "127.0.0.1:"+portFromAddr(addr), conn)
	if err != nil {
		t.Fatalf("ClientHandshake: %v", err)
	}

	select {
	case sni := <-sniChan:
		if sni != "" {
			t.Errorf("SNI = %q, want empty string", sni)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SNI")
	}
}

func TestSNI_IPv6AddressEmptySNI(t *testing.T) {
	cert, pool := generateSelfSignedCert(t)
	wrapped := newSNIStrippingCreds(t, cert, pool)
	addr, sniChan := startTLSListenerCapturingSNI(t, cert)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = wrapped.ClientHandshake(ctx, "[::1]:"+portFromAddr(addr), conn)
	if err != nil {
		t.Fatalf("ClientHandshake: %v", err)
	}

	select {
	case sni := <-sniChan:
		if sni != "" {
			t.Errorf("SNI = %q, want empty string", sni)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SNI")
	}
}

func portFromAddr(addr string) string {
	_, port, _ := net.SplitHostPort(addr)
	return port
}
