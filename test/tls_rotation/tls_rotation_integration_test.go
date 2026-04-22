// Package tls_rotation exercises HTTPS certificate rotation end-to-end:
// start a real `weed master` with an HTTPS listener, capture the leaf
// served at handshake time, rewrite the cert/key files on disk, and
// assert that a subsequent handshake sees the new leaf — all without
// stopping the master process. The test shortens the reloader's refresh
// window to ~half a second via WEED_TLS_CERT_REFRESH_INTERVAL so it
// completes in seconds rather than hours.
package tls_rotation

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestMasterHTTPSCertRotation boots `weed master` with HTTPS, confirms
// the initial leaf is served, rotates the cert/key pair on disk, and
// asserts the rotated leaf is served on subsequent TLS handshakes.
func TestMasterHTTPSCertRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping HTTPS rotation integration test in -short mode")
	}

	weedBin := findWeedBinary(t)

	dir := t.TempDir()
	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0o755); err != nil {
		t.Fatalf("mkdir tls: %v", err)
	}
	certPath := filepath.Join(tlsDir, "server.crt")
	keyPath := filepath.Join(tlsDir, "server.key")

	ca, caKey := generateCA(t)
	leafSerial1 := big.NewInt(10001)
	leafSerial2 := big.NewInt(10002)

	// Initial leaf on disk.
	writeLeaf(t, certPath, keyPath, ca, caKey, leafSerial1)

	masterDir := filepath.Join(dir, "master")
	if err := os.MkdirAll(masterDir, 0o755); err != nil {
		t.Fatalf("mkdir master: %v", err)
	}
	// Empty security.toml so the master doesn't pick up a user's
	// ~/.seaweedfs/security.toml during the test.
	if err := os.WriteFile(filepath.Join(masterDir, "security.toml"), []byte("# test\n"), 0o644); err != nil {
		t.Fatalf("write security.toml: %v", err)
	}

	// Master auto-derives gRPC port as port+10000 when -port.grpc is
	// unset, so both must fit in uint16. Pin both explicitly.
	port, grpcPort := getFreeTCPPort(t), getFreeTCPPort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, weedBin, "master",
		"-ip", "127.0.0.1",
		"-port", strconv.Itoa(port),
		"-port.grpc", strconv.Itoa(grpcPort),
		"-mdir", masterDir,
	)
	cmd.Dir = masterDir
	cmd.Env = append(os.Environ(),
		"WEED_HTTPS_MASTER_CERT="+certPath,
		"WEED_HTTPS_MASTER_KEY="+keyPath,
		// Short refresh window so rotation completes in seconds.
		"WEED_TLS_CERT_REFRESH_INTERVAL=500ms",
	)
	logPath := filepath.Join(masterDir, "master.log")
	logOut, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create master log: %v", err)
	}
	cmd.Stdout = logOut
	cmd.Stderr = logOut

	if err := cmd.Start(); err != nil {
		t.Fatalf("start master: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		}
		_ = logOut.Close()
		if t.Failed() {
			if b, readErr := os.ReadFile(logPath); readErr == nil {
				t.Logf("master.log:\n%s", string(b))
			}
		}
	})

	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// 1. Wait for the initial leaf to appear. Master takes a few seconds
	//    to open its HTTPS listener.
	waitForCert(t, addr, caPool, leafSerial1, 30*time.Second, "initial cert")

	// Sanity: same handshake twice still observes the initial leaf.
	if got := peekServerCert(t, addr, caPool); got == nil || got.SerialNumber.Cmp(leafSerial1) != 0 {
		t.Fatalf("second probe before rotation did not return initial leaf: %v", got)
	}

	// 2. Rotate on disk. pemfile watches mtime, so each file's write is
	//    an atomic rename (tempfile in the same directory).
	writeLeaf(t, certPath, keyPath, ca, caKey, leafSerial2)

	// 3. Wait for new leaf to take over. With a 500ms refresh and no
	//    connection pooling (tls.Dial opens a fresh conn each time), this
	//    should take a couple of seconds.
	waitForCert(t, addr, caPool, leafSerial2, 15*time.Second, "rotated cert")
}

// waitForCert polls until a TLS handshake against addr yields a peer
// cert with the expected serial, or fails the test at the deadline.
func waitForCert(t *testing.T, addr string, caPool *x509.CertPool, wantSerial *big.Int, within time.Duration, label string) {
	t.Helper()
	deadline := time.Now().Add(within)
	var lastErr error
	var lastSerial *big.Int
	for time.Now().Before(deadline) {
		cert := peekServerCert(t, addr, caPool)
		if cert != nil {
			lastSerial = cert.SerialNumber
			if cert.SerialNumber.Cmp(wantSerial) == 0 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s (want serial %s, last seen %v, last err %v)", label, wantSerial, lastSerial, lastErr)
}

// peekServerCert opens a one-shot TLS connection, returns the leaf, and
// closes the connection. Returning nil means the handshake failed; the
// caller treats that as "not ready yet" and keeps polling.
func peekServerCert(t *testing.T, addr string, caPool *x509.CertPool) *x509.Certificate {
	t.Helper()
	d := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := tls.DialWithDialer(d, "tcp", addr, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
	})
	if err != nil {
		return nil
	}
	defer conn.Close()
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil
	}
	return state.PeerCertificates[0]
}

func getFreeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func findWeedBinary(t *testing.T) string {
	t.Helper()
	candidates := []string{
		"../../weed/weed",
		"../weed/weed",
		"./weed",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			abs, absErr := filepath.Abs(c)
			if absErr == nil {
				return abs
			}
			return c
		}
	}
	if path, err := exec.LookPath("weed"); err == nil {
		return path
	}
	t.Skip("weed binary not found — build with `cd weed && go build` first")
	return ""
}

// --- cert fixtures -------------------------------------------------------

// generateCA returns a self-signed CA cert and its private key.
func generateCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tls-rotation-test-CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return parsed, key
}

// writeLeaf signs a new leaf cert with the given serial and writes it
// plus its key to the given paths via atomic rename — the pattern
// Kubernetes (cert-manager → Secret volume mount) produces in practice.
func writeLeaf(t *testing.T, certPath, keyPath string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, serial *big.Int) {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	atomicWritePEM(t, certPath, "CERTIFICATE", der)

	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	atomicWritePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
}

// atomicWritePEM writes a PEM file via tempfile-in-same-directory plus
// rename, matching what kubelet does when it swaps the ..data symlink
// for a renewed Secret. Ensures the reader never sees a truncated file.
func atomicWritePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tls-*")
	if err != nil {
		t.Fatalf("create tempfile: %v", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp.Name())
		}
	}()
	if err := pem.Encode(tmp, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		tmp.Close()
		t.Fatalf("pem encode: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close tempfile: %v", err)
	}
	if err := os.Chmod(tmp.Name(), 0o600); err != nil {
		t.Fatalf("chmod tempfile: %v", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		t.Fatalf("rename tempfile onto %s: %v", path, err)
	}
	ok = true
}
