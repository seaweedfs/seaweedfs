package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/viper"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security/certreload"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/tls/certprovider/pemfile"
	"google.golang.org/grpc/security/advancedtls"
)

// CredRefreshingInterval is the refresh cadence for gRPC mTLS certs.
// Shares its source of truth with certreload.DefaultRefreshInterval so
// a single WEED_TLS_CERT_REFRESH_INTERVAL env var tunes both gRPC and
// HTTPS cert reload.
var CredRefreshingInterval = certreload.DefaultRefreshInterval

type Authenticator struct {
	AllowedWildcardDomain string
	AllowedCommonNames    map[string]bool
}

// SNIStrippingTransportCredentials wraps another TransportCredentials
// and strips the port from the authority in ClientHandshake to prevent
// advancedtls from using the full "host:port" as ServerName in SNI.
type SNIStrippingTransportCredentials struct {
	creds credentials.TransportCredentials
}

func (s *SNIStrippingTransportCredentials) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	host, _, err := net.SplitHostPort(authority)
	if err == nil {
		authority = host
	}
	return s.creds.ClientHandshake(ctx, authority, rawConn)
}

func (s *SNIStrippingTransportCredentials) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return s.creds.ServerHandshake(rawConn)
}

func (s *SNIStrippingTransportCredentials) Info() credentials.ProtocolInfo {
	return s.creds.Info()
}

func (s *SNIStrippingTransportCredentials) Clone() credentials.TransportCredentials {
	return &SNIStrippingTransportCredentials{creds: s.creds.Clone()}
}

func (s *SNIStrippingTransportCredentials) OverrideServerName(serverNameOverride string) error {
	return s.creds.OverrideServerName(serverNameOverride)
}

func LoadServerTLS(config *util.ViperProxy, component string) (grpc.ServerOption, grpc.ServerOption) {
	creds, verify, err := loadServerTLS(config, component)
	if err != nil {
		glog.Fatalf("gRPC server TLS for %s: %v", component, err)
	}
	return creds, verify
}

func loadServerTLS(config *util.ViperProxy, component string) (grpc.ServerOption, grpc.ServerOption, error) {
	if config == nil {
		return nil, nil, nil
	}

	serverOptions := pemfile.Options{
		CertFile:        config.GetString(component + ".cert"),
		KeyFile:         config.GetString(component + ".key"),
		RefreshDuration: CredRefreshingInterval,
	}
	if serverOptions.CertFile == "" && serverOptions.KeyFile == "" {
		return nil, nil, nil
	}
	if serverOptions.CertFile == "" || serverOptions.KeyFile == "" {
		return nil, nil, fmt.Errorf("%s.cert and %s.key must both be set", component, component)
	}
	for _, p := range []string{serverOptions.CertFile, serverOptions.KeyFile, config.GetString("grpc.ca")} {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err != nil {
			return nil, nil, fmt.Errorf("stat TLS file %s: %w", p, err)
		}
	}

	serverIdentityProvider, err := pemfile.NewProvider(serverOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("pemfile.NewProvider(%v) failed: %w", serverOptions, err)
	}

	serverRootOptions := pemfile.Options{
		RootFile:        config.GetString("grpc.ca"),
		RefreshDuration: CredRefreshingInterval,
	}
	serverRootProvider, err := pemfile.NewProvider(serverRootOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("pemfile.NewProvider(%v) failed: %w", serverRootOptions, err)
	}

	// Start a server and create a client using advancedtls API with Provider.
	options := &advancedtls.Options{
		IdentityOptions: advancedtls.IdentityCertificateOptions{
			IdentityProvider: serverIdentityProvider,
		},
		RootOptions: advancedtls.RootCertificateOptions{
			RootProvider: serverRootProvider,
		},
		RequireClientCert: true,
		VerificationType:  advancedtls.CertVerification,
	}
	options.MinTLSVersion, err = TlsVersionByName(config.GetString("tls.min_version"))
	if err != nil {
		return nil, nil, err
	}
	options.MaxTLSVersion, err = TlsVersionByName(config.GetString("tls.max_version"))
	if err != nil {
		return nil, nil, err
	}
	options.CipherSuites, err = TlsCipherSuiteByNames(config.GetString("tls.cipher_suites"))
	if err != nil {
		return nil, nil, err
	}
	auther := Authenticator{
		AllowedCommonNames:    parseAllowedCommonNames(config.GetString(component + ".allowed_commonNames")),
		AllowedWildcardDomain: config.GetString("grpc.allowed_wildcard_domain"),
	}
	if len(auther.AllowedCommonNames) != 0 || auther.AllowedWildcardDomain != "" {
		options.AdditionalPeerVerification = auther.Authenticate
	} else {
		options.AdditionalPeerVerification = func(params *advancedtls.HandshakeVerificationInfo) (*advancedtls.PostHandshakeVerificationResults, error) {
			return &advancedtls.PostHandshakeVerificationResults{}, nil
		}
	}
	ta, err := advancedtls.NewServerCreds(options)
	if err != nil {
		return nil, nil, fmt.Errorf("advancedtls.NewServerCreds failed: %w", err)
	}
	return grpc.Creds(ta), nil, nil
}

func LoadClientTLSFromFile(configFile string, component string) (grpc.DialOption, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read security config %s: %v", configFile, err)
	}
	// Resolve relative PEM paths against the config file's directory.
	configDir := filepath.Dir(configFile)
	for _, key := range []string{"grpc.ca", component + ".cert", component + ".key", component + ".client_cert", component + ".client_key"} {
		p := v.GetString(key)
		if p != "" && !filepath.IsAbs(p) {
			v.Set(key, filepath.Join(configDir, p))
		}
	}
	return LoadClientTLS(util.NewViperProxy(v), component), nil
}

func LoadClientTLS(config *util.ViperProxy, component string) grpc.DialOption {
	option, err := loadClientTLS(config, component)
	if err != nil {
		glog.Fatalf("gRPC client TLS for %s: %v", component, err)
	}
	return option
}

func loadClientTLS(config *util.ViperProxy, component string) (grpc.DialOption, error) {
	if config == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}

	// prefer a dedicated client certificate: CAs may issue certs with only one of the serverAuth/clientAuth EKUs
	certFileName, keyFileName := config.GetString(component+".client_cert"), config.GetString(component+".client_key")
	if certFileName == "" || keyFileName == "" {
		if certFileName != "" || keyFileName != "" {
			glog.Warningf("%s.client_cert and %s.client_key must both be set, falling back to %s.cert and %s.key", component, component, component, component)
		}
		certFileName, keyFileName = config.GetString(component+".cert"), config.GetString(component+".key")
	}
	if certFileName == "" && keyFileName == "" {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	if certFileName == "" || keyFileName == "" {
		return nil, fmt.Errorf("%s.cert and %s.key must both be set", component, component)
	}
	caFileName := config.GetString("grpc.ca")
	if caFileName == "" {
		return nil, fmt.Errorf("%s cert and key are set but grpc.ca is not", component)
	}
	for _, p := range []string{certFileName, keyFileName, caFileName} {
		if _, err := os.Stat(p); err != nil {
			return nil, fmt.Errorf("stat TLS file %s: %w", p, err)
		}
	}

	clientOptions := pemfile.Options{
		CertFile:        certFileName,
		KeyFile:         keyFileName,
		RefreshDuration: CredRefreshingInterval,
	}
	clientProvider, err := pemfile.NewProvider(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("pemfile.NewProvider(%v) failed: %w", clientOptions, err)
	}
	clientRootOptions := pemfile.Options{
		RootFile:        caFileName,
		RefreshDuration: CredRefreshingInterval,
	}
	clientRootProvider, err := pemfile.NewProvider(clientRootOptions)
	if err != nil {
		return nil, fmt.Errorf("pemfile.NewProvider(%v) failed: %w", clientRootOptions, err)
	}
	options := &advancedtls.Options{
		IdentityOptions: advancedtls.IdentityCertificateOptions{
			IdentityProvider: clientProvider,
		},
		AdditionalPeerVerification: func(params *advancedtls.HandshakeVerificationInfo) (*advancedtls.PostHandshakeVerificationResults, error) {
			return &advancedtls.PostHandshakeVerificationResults{}, nil
		},
		RootOptions: advancedtls.RootCertificateOptions{
			RootProvider: clientRootProvider,
		},
		VerificationType: advancedtls.CertVerification,
	}
	ta, err := advancedtls.NewClientCreds(options)
	if err != nil {
		return nil, fmt.Errorf("advancedtls.NewClientCreds failed: %w", err)
	}
	wrapped := &SNIStrippingTransportCredentials{creds: ta}
	return grpc.WithTransportCredentials(wrapped), nil
}

// LoadHTTPClientFromFile creates an HTTP client using the https.client TLS
// settings from the given security config file. Returns nil if HTTPS is not
// enabled in the config. This is used by filer.sync to create per-cluster
// HTTP clients when clusters use different certificates.
func LoadHTTPClientFromFile(configFile string) (*util_http_client.HTTPClient, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read security config %s: %v", configFile, err)
	}

	if !v.GetBool("https.client.enabled") {
		return nil, nil
	}

	configDir := filepath.Dir(configFile)
	resolvePath := func(key string) string {
		p := v.GetString(key)
		if p != "" && !filepath.IsAbs(p) {
			return filepath.Join(configDir, p)
		}
		return p
	}

	return util_http_client.NewHttpClientWithTLS(
		resolvePath("https.client.cert"),
		resolvePath("https.client.key"),
		resolvePath("https.client.ca"),
		v.GetBool("https.client.insecure_skip_verify"),
		util_http_client.AddDialContext,
	)
}

func LoadClientTLSHTTP(clientCertFile string) *tls.Config {
	clientCerts, err := os.ReadFile(clientCertFile)
	if err != nil {
		glog.Fatal(err)
	}
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(clientCerts)
	if !ok {
		glog.Fatalf("Error processing client certificate in %s\n", clientCertFile)
	}

	return &tls.Config{
		ClientCAs:  certPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
}

func parseAllowedCommonNames(commonNames string) map[string]bool {
	allowed := make(map[string]bool)
	for _, s := range strings.Split(commonNames, ",") {
		if s = strings.TrimSpace(s); s != "" {
			allowed[s] = true
		}
	}
	return allowed
}

func (a Authenticator) Authenticate(params *advancedtls.HandshakeVerificationInfo) (*advancedtls.PostHandshakeVerificationResults, error) {
	if a.AllowedWildcardDomain != "" && strings.HasSuffix(params.Leaf.Subject.CommonName, a.AllowedWildcardDomain) {
		return &advancedtls.PostHandshakeVerificationResults{}, nil
	}
	if _, ok := a.AllowedCommonNames[params.Leaf.Subject.CommonName]; ok {
		return &advancedtls.PostHandshakeVerificationResults{}, nil
	}
	err := fmt.Errorf("Authenticate: invalid subject client common name: %s", params.Leaf.Subject.CommonName)
	glog.Error(err)
	return nil, err
}

func FixTlsConfig(viper *util.ViperProxy, config *tls.Config) error {
	var err error
	config.MinVersion, err = TlsVersionByName(viper.GetString("tls.min_version"))
	if err != nil {
		return err
	}
	config.MaxVersion, err = TlsVersionByName(viper.GetString("tls.max_version"))
	if err != nil {
		return err
	}
	config.CipherSuites, err = TlsCipherSuiteByNames(viper.GetString("tls.cipher_suites"))
	return err
}

func TlsVersionByName(name string) (uint16, error) {
	switch name {
	case "":
		return 0, nil
	case "SSLv3":
		return tls.VersionSSL30, nil
	case "TLS 1.0":
		return tls.VersionTLS10, nil
	case "TLS 1.1":
		return tls.VersionTLS11, nil
	case "TLS 1.2":
		return tls.VersionTLS12, nil
	case "TLS 1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("invalid tls version %s", name)
	}
}

func TlsCipherSuiteByNames(cipherSuiteNames string) ([]uint16, error) {
	cipherSuiteNames = strings.TrimSpace(cipherSuiteNames)
	if cipherSuiteNames == "" {
		return nil, nil
	}
	names := strings.Split(cipherSuiteNames, ",")
	cipherSuites := tls.CipherSuites()
	cipherIds := make([]uint16, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		index := slices.IndexFunc(cipherSuites, func(suite *tls.CipherSuite) bool {
			return name == suite.Name
		})
		if index == -1 {
			return nil, fmt.Errorf("invalid tls cipher suite name %s", name)
		}
		cipherIds = append(cipherIds, cipherSuites[index].ID)
	}
	return cipherIds, nil
}
