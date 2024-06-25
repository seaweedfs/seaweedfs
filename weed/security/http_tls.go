package security

import (
	"crypto/x509"
	"fmt"
	util "github.com/seaweedfs/seaweedfs/weed/util"
	util_services "github.com/seaweedfs/seaweedfs/weed/util/services"
	"github.com/spf13/viper"
	"net/url"
	"strings"
	"sync"
)

var (
	certPools = make(map[util_services.Name]*x509.CertPool)
	mu        sync.Mutex
)

func init() {
	if !util.LoadConfiguration("security", false) {
		return
	}

	for _, service := range util_services.GetAll() {
		if caCertFilePath := viper.GetString(fmt.Sprintf("https.%s.ca", service.LowerCaseString())); caCertFilePath != "" {
			tlsConfig := LoadClientTLSHTTP(caCertFilePath)
			mu.Lock()
			defer mu.Unlock()
			certPools[service] = tlsConfig.ClientCAs
		}
	}
}

func GetMasterCertPool() *x509.CertPool {
	return getCertPoolByServiceName(util_services.Master)
}

func GetVolumeCertPool() *x509.CertPool {
	return getCertPoolByServiceName(util_services.Volume)
}

func GetFilerCertPool() *x509.CertPool {
	return getCertPoolByServiceName(util_services.Filer)
}

func GetMasterHttpScheme() string {
	return getHttpShemeByServiceName(util_services.Master)
}

func GetVolumeHttpScheme() string {
	return getHttpShemeByServiceName(util_services.Volume)
}

func GetFilerHttpScheme() string {
	return getHttpShemeByServiceName(util_services.Filer)
}

func ToMasterHttpScheme(rawURL string) (string, error) {
	return fixHttpShemeByServiceName(util_services.Master, rawURL)
}

func ToVolumeHttpScheme(rawURL string) (string, error) {
	return fixHttpShemeByServiceName(util_services.Volume, rawURL)
}

func ToFilerHttpScheme(rawURL string) (string, error) {
	return fixHttpShemeByServiceName(util_services.Filer, rawURL)
}

func getCertPoolByServiceName(serviceName util_services.Name) *x509.CertPool {
	mu.Lock()
	defer mu.Unlock()
	if pool, exists := certPools[serviceName]; exists {
		return pool
	}
	return nil
}

func getHttpShemeByServiceName(serviceName util_services.Name) string {
	certPool := getCertPoolByServiceName(serviceName)
	if certPool == nil {
		return "http"
	}
	return "https"
}

func fixHttpShemeByServiceName(serviceName util_services.Name, rawURL string) (string, error) {
	expectedScheme := getHttpShemeByServiceName(serviceName)

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
