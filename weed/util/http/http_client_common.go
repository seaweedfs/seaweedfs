package http

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	util "github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

var (
	once sync.Once
)

type ClientCfg struct {
	Client            *http.Client
	Transport         *http.Transport
	expectHttpsScheme bool
}

func (cfg *ClientCfg) GetHttpScheme() string {
	if cfg.expectHttpsScheme {
		return "https"
	}
	return "http"
}

func (cfg *ClientCfg) FixHttpScheme(rawURL string) (string, error) {
	expectedScheme := cfg.GetHttpScheme()

	if !(strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://")) {
		return expectedScheme + "://" + rawURL, nil
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	if expectedScheme != parsedURL.Scheme {
		parsedURL.Scheme = expectedScheme
	}
	return parsedURL.String(), nil
}

func NewClientCfg(serviceName ServiceName, opts ...NewClientCfgOpt) (*ClientCfg, error) {
	clientCfg := ClientCfg{}
	clientCfg.expectHttpsScheme = false

	clientCertPair, err := GetClientCertPair(serviceName)
	if err != nil {
		return nil, err
	}

	clientCaCert, clientCaCertName, err := GetClientCaCert(serviceName)
	if err != nil {
		return nil, err
	}

	var tlsConfig *tls.Config = nil
	if clientCertPair != nil || len(clientCaCert) != 0 {
		clientCfg.expectHttpsScheme = true
		caCertPool, err := CreateHTTPClientCertPool(clientCaCert, clientCaCertName)
		if err != nil {
			return nil, err
		}

		tlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{},
			RootCAs:            caCertPool,
			InsecureSkipVerify: false,
		}

		if clientCertPair != nil {
			tlsConfig.Certificates = append(tlsConfig.Certificates, *clientCertPair)
		}
	}

	clientCfg.Transport = &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		TLSClientConfig:     tlsConfig,
	}
	clientCfg.Client = &http.Client{
		Transport: clientCfg.Transport,
	}

	for _, opt := range opts {
		opt(&clientCfg)
	}
	return &clientCfg, nil
}

func GetFileNameFromSecurityConfiguration(serviceName ServiceName, fileType string) string {
	once.Do(func() {
		util.LoadConfiguration("security", false)
	})
	return viper.GetString(fmt.Sprintf("https.%s.%s", serviceName.LowerCaseString(), fileType))
}

func GetFileContentFromSecurityConfiguration(serviceName ServiceName, fileType string) ([]byte, string, error) {
	if fileName := GetFileNameFromSecurityConfiguration(serviceName, fileType); fileName != "" {
		fileContent, err := os.ReadFile(fileName)
		if err != nil {
			return nil, fileName, err
		}
		return fileContent, fileName, err
	}
	return nil, "", nil
}

func GetClientCertPair(serviceName ServiceName) (*tls.Certificate, error) {
	certFileName := GetFileNameFromSecurityConfiguration(serviceName, "cert")
	keyFileName := GetFileNameFromSecurityConfiguration(serviceName, "key")
	if certFileName == "" && keyFileName == "" {
		return nil, nil
	}
	if certFileName != "" && keyFileName != "" {
		clientCert, err := tls.LoadX509KeyPair(certFileName, keyFileName)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate and key: %s", err)
		}
		return &clientCert, nil
	}
	return nil, fmt.Errorf("error loading key pair: key `%s` and certificate `%s`", keyFileName, certFileName)
}

func GetClientCaCert(serviceName ServiceName) ([]byte, string, error) {
	return GetFileContentFromSecurityConfiguration(serviceName, "ca")
}

func CreateHTTPClientCertPool(certContent []byte, fileName string) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()
	if len(certContent) == 0 {
		return certPool, nil
	}

	ok := certPool.AppendCertsFromPEM(certContent)
	if !ok {
		return nil, fmt.Errorf("error processing certificate in %s", fileName)
	}
	return certPool, nil
}
