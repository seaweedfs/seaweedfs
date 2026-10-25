package security

import (
	"crypto/tls"

	"github.com/seaweedfs/seaweedfs/weed/security/certreload"
	"google.golang.org/grpc/credentials/tls/certprovider"
)

// NewReloadingServerCertificate returns a GetCertificate callback for
// tls.Config.GetCertificate, backed by a refreshing pem provider so
// rotated certs (e.g. from Kubernetes cert-manager) are picked up
// without a restart.
//
// Thin wrapper over certreload; the shared implementation lives in the
// subpackage so both server TLS (this package) and HTTP client TLS
// (weed/util/http/client) can use it without an import cycle.
func NewReloadingServerCertificate(certFile, keyFile string) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), certprovider.Provider, error) {
	return certreload.NewServerGetCertificate(certFile, keyFile)
}
