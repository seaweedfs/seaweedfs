package security

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc/credentials/tls/certprovider"
	"google.golang.org/grpc/credentials/tls/certprovider/pemfile"
)

// NewReloadingServerCertificate watches certFile and keyFile on disk and
// returns a GetCertificate callback suitable for tls.Config.GetCertificate.
// The callback picks up rotated certificates without restarting the server,
// which is required under Kubernetes cert-manager.
//
// The refresh cadence is controlled by CredRefreshingInterval. The provider
// re-reads the files only when their mtime/contents change, so the callback
// is cheap on the hot path.
//
// Callers that keep a handle to the provider should Close() it at shutdown
// to stop the background goroutine.
func NewReloadingServerCertificate(certFile, keyFile string) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), certprovider.Provider, error) {
	return newReloadingServerCertificate(certFile, keyFile, CredRefreshingInterval)
}

func newReloadingServerCertificate(certFile, keyFile string, refresh time.Duration) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), certprovider.Provider, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil, fmt.Errorf("both certFile and keyFile are required")
	}
	provider, err := pemfile.NewProvider(pemfile.Options{
		CertFile:        certFile,
		KeyFile:         keyFile,
		RefreshDuration: refresh,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pemfile.NewProvider: %w", err)
	}
	get := func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		km, err := provider.KeyMaterial(context.Background())
		if err != nil {
			return nil, err
		}
		if km == nil || len(km.Certs) == 0 {
			return nil, fmt.Errorf("no TLS key material available for %s", certFile)
		}
		cert := km.Certs[0]
		return &cert, nil
	}
	return get, provider, nil
}
