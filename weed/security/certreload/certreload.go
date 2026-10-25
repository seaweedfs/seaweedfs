// Package certreload wraps grpc's pemfile.Provider so both TLS servers
// (weed/security) and TLS clients (weed/util/http/client) can share one
// reloading cert implementation without an import cycle between them.
//
// Lives in its own subpackage because weed/security already imports
// weed/util/http/client (for LoadHTTPClientFromFile).
package certreload

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc/credentials/tls/certprovider"
	"google.golang.org/grpc/credentials/tls/certprovider/pemfile"
)

// RefreshIntervalEnv names an environment variable that overrides the
// refresh cadence. Accepts any time.ParseDuration value (e.g. "30m",
// "500ms"). Primarily a hook for integration tests that need rotation
// to complete in seconds, but also useful in production when paired
// with short-lived certs (e.g. Vault-issued).
const RefreshIntervalEnv = "WEED_TLS_CERT_REFRESH_INTERVAL"

// DefaultRefreshInterval is the cadence at which the pemfile provider
// stats cert/key files on disk. It re-parses only when mtime/contents
// change, so the hot path (KeyMaterial() on each TLS handshake) stays
// cheap.
//
// 5 hours matches the prior constant used for gRPC mTLS. Resolved once
// at process start from RefreshIntervalEnv if set.
var DefaultRefreshInterval = resolveRefreshInterval(5 * time.Hour)

func resolveRefreshInterval(fallback time.Duration) time.Duration {
	if s := os.Getenv(RefreshIntervalEnv); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

// NewServerGetCertificate returns a callback suitable for
// tls.Config.GetCertificate. It reloads certFile and keyFile from disk
// on each refresh tick so rotated certs (e.g. from k8s cert-manager)
// are picked up without a restart. Caller should Close() the returned
// provider at shutdown.
func NewServerGetCertificate(certFile, keyFile string) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), certprovider.Provider, error) {
	return newServerGetCertificate(certFile, keyFile, DefaultRefreshInterval)
}

func newServerGetCertificate(certFile, keyFile string, refresh time.Duration) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), certprovider.Provider, error) {
	provider, err := newProvider(certFile, keyFile, refresh)
	if err != nil {
		return nil, nil, err
	}
	get := func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		// KeyMaterial blocks until the pemfile provider has read the files
		// for the first time. Use the handshake context so a stuck read
		// bounds the handshake instead of hanging it forever.
		ctx := context.Background()
		if hello != nil {
			ctx = hello.Context()
		}
		return current(ctx, provider, certFile)
	}
	return get, provider, nil
}

// NewClientGetCertificate returns a callback suitable for
// tls.Config.GetClientCertificate. Fires per TLS handshake, so long-lived
// HTTPS clients (FUSE mount, backup, filer→volume, etc.) pick up rotated
// client mTLS certs as pooled connections recycle.
func NewClientGetCertificate(certFile, keyFile string) (func(*tls.CertificateRequestInfo) (*tls.Certificate, error), certprovider.Provider, error) {
	return newClientGetCertificate(certFile, keyFile, DefaultRefreshInterval)
}

func newClientGetCertificate(certFile, keyFile string, refresh time.Duration) (func(*tls.CertificateRequestInfo) (*tls.Certificate, error), certprovider.Provider, error) {
	provider, err := newProvider(certFile, keyFile, refresh)
	if err != nil {
		return nil, nil, err
	}
	get := func(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		ctx := context.Background()
		if cri != nil {
			ctx = cri.Context()
		}
		return current(ctx, provider, certFile)
	}
	return get, provider, nil
}

func newProvider(certFile, keyFile string, refresh time.Duration) (certprovider.Provider, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("both certFile and keyFile are required")
	}
	provider, err := pemfile.NewProvider(pemfile.Options{
		CertFile:        certFile,
		KeyFile:         keyFile,
		RefreshDuration: refresh,
	})
	if err != nil {
		return nil, fmt.Errorf("pemfile.NewProvider: %w", err)
	}
	return provider, nil
}

func current(ctx context.Context, provider certprovider.Provider, certFile string) (*tls.Certificate, error) {
	km, err := provider.KeyMaterial(ctx)
	if err != nil {
		return nil, err
	}
	if km == nil || len(km.Certs) == 0 {
		return nil, fmt.Errorf("no TLS key material available for %s", certFile)
	}
	cert := km.Certs[0]
	return &cert, nil
}
