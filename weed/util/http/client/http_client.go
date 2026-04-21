package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/security/certreload"
	util "github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
)

var (
	loadSecurityConfigOnce sync.Once
)

type HTTPClient struct {
	Client            *http.Client
	Transport         *http.Transport
	expectHttpsScheme bool
}

func (httpClient *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = httpClient.GetHttpScheme()
	return httpClient.Client.Do(req)
}

func (httpClient *HTTPClient) Get(url string) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)
	if err != nil {
		return nil, err
	}
	return httpClient.Client.Get(url)
}

func (httpClient *HTTPClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)
	if err != nil {
		return nil, err
	}
	return httpClient.Client.Post(url, contentType, body)
}

func (httpClient *HTTPClient) PostForm(url string, data url.Values) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)
	if err != nil {
		return nil, err
	}
	return httpClient.Client.PostForm(url, data)
}

func (httpClient *HTTPClient) Head(url string) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)
	if err != nil {
		return nil, err
	}
	return httpClient.Client.Head(url)
}
func (httpClient *HTTPClient) CloseIdleConnections() {
	httpClient.Client.CloseIdleConnections()
}

func (httpClient *HTTPClient) GetClientTransport() *http.Transport {
	return httpClient.Transport
}

func (httpClient *HTTPClient) GetHttpScheme() string {
	if httpClient.expectHttpsScheme {
		return "https"
	}
	return "http"
}

func (httpClient *HTTPClient) NormalizeHttpScheme(rawURL string) (string, error) {
	expectedScheme := httpClient.GetHttpScheme()

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

func NewHttpClient(clientName ClientName, opts ...HttpClientOpt) (*HTTPClient, error) {
	httpClient := HTTPClient{}
	httpClient.expectHttpsScheme = checkIsHttpsClientEnabled(clientName)
	var tlsConfig *tls.Config = nil

	if httpClient.expectHttpsScheme {
		certFileName, keyFileName, hasClientCert, err := clientCertPaths(clientName)
		if err != nil {
			return nil, err
		}

		clientCaCert, clientCaCertName, err := getClientCaCert(clientName)
		if err != nil {
			return nil, err
		}

		if hasClientCert || len(clientCaCert) != 0 {
			caCertPool, err := createHTTPClientCertPool(clientCaCert, clientCaCertName)
			if err != nil {
				return nil, err
			}

			tlsConfig = &tls.Config{
				RootCAs:            caCertPool,
				InsecureSkipVerify: false,
			}

			if hasClientCert {
				getClientCert, _, err := certreload.NewClientGetCertificate(certFileName, keyFileName)
				if err != nil {
					return nil, fmt.Errorf("error loading client certificate and key: %s", err)
				}
				tlsConfig.GetClientCertificate = getClientCert
			}
		}

		if getBoolOptionFromSecurityConfiguration(clientName, "insecure_skip_verify") {
			if tlsConfig == nil {
				tlsConfig = &tls.Config{}
			}
			tlsConfig.InsecureSkipVerify = true
		}
	}

	httpClient.Transport = &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		TLSClientConfig:     tlsConfig,
	}
	httpClient.Client = &http.Client{
		Transport: httpClient.Transport,
	}

	for _, opt := range opts {
		opt(&httpClient)
	}
	return &httpClient, nil
}

func getStringOptionFromSecurityConfiguration(clientName ClientName, stringOptionName string) string {
	util.LoadSecurityConfiguration()
	return viper.GetString(fmt.Sprintf("https.%s.%s", clientName.LowerCaseString(), stringOptionName))
}

func getBoolOptionFromSecurityConfiguration(clientName ClientName, boolOptionName string) bool {
	util.LoadSecurityConfiguration()
	return viper.GetBool(fmt.Sprintf("https.%s.%s", clientName.LowerCaseString(), boolOptionName))
}

func checkIsHttpsClientEnabled(clientName ClientName) bool {
	return getBoolOptionFromSecurityConfiguration(clientName, "enabled")
}

func getFileContentFromSecurityConfiguration(clientName ClientName, fileType string) ([]byte, string, error) {
	if fileName := getStringOptionFromSecurityConfiguration(clientName, fileType); fileName != "" {
		fileContent, err := os.ReadFile(fileName)
		if err != nil {
			return nil, fileName, err
		}
		return fileContent, fileName, err
	}
	return nil, "", nil
}

// clientCertPaths reads the https.<clientName>.{cert,key} paths from the
// security config, validates they're either both set or both empty, and
// returns them along with a hasClientCert flag. Loading is deferred to
// certreload so the cert/key pair is picked up from disk on rotation.
func clientCertPaths(clientName ClientName) (certFile, keyFile string, hasClientCert bool, err error) {
	certFile = getStringOptionFromSecurityConfiguration(clientName, "cert")
	keyFile = getStringOptionFromSecurityConfiguration(clientName, "key")
	if certFile == "" && keyFile == "" {
		return "", "", false, nil
	}
	if certFile == "" || keyFile == "" {
		return "", "", false, fmt.Errorf("https.%s: both cert and key must be set (got cert=%q key=%q)", clientName.LowerCaseString(), certFile, keyFile)
	}
	return certFile, keyFile, true, nil
}

func getClientCaCert(clientName ClientName) ([]byte, string, error) {
	return getFileContentFromSecurityConfiguration(clientName, "ca")
}

// NewHttpClientWithTLS creates an HTTPClient with explicit TLS certificate
// parameters instead of reading from the global security configuration.
// This is used by filer.sync to create per-cluster HTTP clients when clusters
// use different certificates.
func NewHttpClientWithTLS(certFile, keyFile, caFile string, insecureSkipVerify bool, opts ...HttpClientOpt) (*HTTPClient, error) {
	httpClient := HTTPClient{}
	httpClient.expectHttpsScheme = true
	var tlsConfig *tls.Config

	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("both cert and key are required for mTLS, got cert=%q key=%q", certFile, keyFile)
	}

	var getClientCert func(*tls.CertificateRequestInfo) (*tls.Certificate, error)
	if certFile != "" && keyFile != "" {
		cb, _, err := certreload.NewClientGetCertificate(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate and key: %s", err)
		}
		getClientCert = cb
	}

	var caCertPool *x509.CertPool
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("error reading CA cert %s: %s", caFile, err)
		}
		caCertPool, err = createHTTPClientCertPool(caCert, caFile)
		if err != nil {
			return nil, err
		}
	}

	if getClientCert != nil || caCertPool != nil || insecureSkipVerify {
		tlsConfig = &tls.Config{
			GetClientCertificate: getClientCert,
			RootCAs:              caCertPool,
			InsecureSkipVerify:   insecureSkipVerify,
		}
	}

	httpClient.Transport = &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		TLSClientConfig:     tlsConfig,
	}
	httpClient.Client = &http.Client{
		Transport: httpClient.Transport,
	}

	for _, opt := range opts {
		opt(&httpClient)
	}
	return &httpClient, nil
}

func createHTTPClientCertPool(certContent []byte, fileName string) (*x509.CertPool, error) {
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
