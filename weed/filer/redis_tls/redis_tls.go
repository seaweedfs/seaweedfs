// Package redis_tls builds the TLS configuration shared by the redis filer stores.
package redis_tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Config reads the TLS options of a redis filer store section and returns nil when TLS is off.
// ServerName is left unset so go-redis derives it from each dialed address, which the sentinel
// and cluster stores need since they talk to more than one host.
func Config(configuration util.Configuration, prefix string) (*tls.Config, error) {

	// enable_mtls is the name the redis2 and redis3 stores shipped with
	if !configuration.GetBool(prefix+"enable_tls") && !configuration.GetBool(prefix+"enable_mtls") {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if caCertPath := configuration.GetString(prefix + "ca_cert_path"); caCertPath != "" {
		caCertBytes, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate %s: %w", caCertPath, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCertBytes) {
			return nil, fmt.Errorf("no CA certificate found in %s", caCertPath)
		}
		tlsConfig.RootCAs = caCertPool
	}

	clientCertPath := configuration.GetString(prefix + "client_cert_path")
	clientKeyPath := configuration.GetString(prefix + "client_key_path")
	if clientCertPath != "" || clientKeyPath != "" {
		clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load client certificate %s and key %s: %w", clientCertPath, clientKeyPath, err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	return tlsConfig, nil
}
