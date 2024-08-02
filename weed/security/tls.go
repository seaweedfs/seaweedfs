package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/tls/certprovider/pemfile"
	"google.golang.org/grpc/security/advancedtls"
)

const CredRefreshingInterval = time.Duration(5) * time.Hour

type Authenticator struct {
	AllowedWildcardDomain string
	AllowedCommonNames    map[string]bool
}

func LoadServerTLS(config *util.ViperProxy, component string) (grpc.ServerOption, grpc.ServerOption) {
	if config == nil {
		return nil, nil
	}

	serverOptions := pemfile.Options{
		CertFile:        config.GetString(component + ".cert"),
		KeyFile:         config.GetString(component + ".key"),
		RefreshDuration: CredRefreshingInterval,
	}
	if serverOptions.CertFile == "" || serverOptions.KeyFile == "" {
		return nil, nil
	}

	serverIdentityProvider, err := pemfile.NewProvider(serverOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) %v failed: %v", serverOptions, component, err)
		return nil, nil
	}

	serverRootOptions := pemfile.Options{
		RootFile:        config.GetString("grpc.ca"),
		RefreshDuration: CredRefreshingInterval,
	}
	serverRootProvider, err := pemfile.NewProvider(serverRootOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) failed: %v", serverRootOptions, err)
		return nil, nil
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
		glog.Warningf("tls min version parse failed, %v", err)
		return nil, nil
	}
	options.MaxTLSVersion, err = TlsVersionByName(config.GetString("tls.max_version"))
	if err != nil {
		glog.Warningf("tls max version parse failed, %v", err)
		return nil, nil
	}
	options.CipherSuites, err = TlsCipherSuiteByNames(config.GetString("tls.cipher_suites"))
	if err != nil {
		glog.Warningf("tls cipher suite parse failed, %v", err)
		return nil, nil
	}
	allowedCommonNames := config.GetString(component + ".allowed_commonNames")
	allowedWildcardDomain := config.GetString("grpc.allowed_wildcard_domain")
	if allowedCommonNames != "" || allowedWildcardDomain != "" {
		allowedCommonNamesMap := make(map[string]bool)
		for _, s := range strings.Split(allowedCommonNames, ",") {
			allowedCommonNamesMap[s] = true
		}
		auther := Authenticator{
			AllowedCommonNames:    allowedCommonNamesMap,
			AllowedWildcardDomain: allowedWildcardDomain,
		}
		options.AdditionalPeerVerification = auther.Authenticate
	} else {
		options.AdditionalPeerVerification = func(params *advancedtls.HandshakeVerificationInfo) (*advancedtls.PostHandshakeVerificationResults, error) {
			return &advancedtls.PostHandshakeVerificationResults{}, nil
		}
	}
	ta, err := advancedtls.NewServerCreds(options)
	if err != nil {
		glog.Warningf("advancedtls.NewServerCreds(%v) failed: %v", options, err)
		return nil, nil
	}
	return grpc.Creds(ta), nil
}

func LoadClientTLS(config *util.ViperProxy, component string) grpc.DialOption {
	if config == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	certFileName, keyFileName, caFileName := config.GetString(component+".cert"), config.GetString(component+".key"), config.GetString("grpc.ca")
	if certFileName == "" || keyFileName == "" || caFileName == "" {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	clientOptions := pemfile.Options{
		CertFile:        certFileName,
		KeyFile:         keyFileName,
		RefreshDuration: CredRefreshingInterval,
	}
	clientProvider, err := pemfile.NewProvider(clientOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) failed %v", clientOptions, err)
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	clientRootOptions := pemfile.Options{
		RootFile:        config.GetString("grpc.ca"),
		RefreshDuration: CredRefreshingInterval,
	}
	clientRootProvider, err := pemfile.NewProvider(clientRootOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) failed: %v", clientRootOptions, err)
		return grpc.WithTransportCredentials(insecure.NewCredentials())
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
		glog.Warningf("advancedtls.NewClientCreds(%v) failed: %v", options, err)
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	return grpc.WithTransportCredentials(ta)
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
