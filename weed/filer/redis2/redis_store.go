package redis2

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &Redis2Store{})
}

type Redis2Store struct {
	UniversalRedis2Store
}

func (store *Redis2Store) GetName() string {
	return "redis2"
}

func (store *Redis2Store) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"address"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
		configuration.GetBool(prefix+"enable_mtls"),
		configuration.GetString(prefix+"ca_cert_path"),
		configuration.GetString(prefix+"client_cert_path"),
		configuration.GetString(prefix+"client_key_path"),
	)
}

func (store *Redis2Store) initialize(hostPort string, password string, database int, superLargeDirectories []string, enableMtls bool, caCertPath string, clientCertPath string, clientKeyPath string) (err error) {
	if enableMtls {
		clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			glog.Fatalf("Error loading client certificate and key pair: %v", err)
		}

		caCertBytes, err := os.ReadFile(caCertPath)
		if err != nil {
			glog.Fatalf("Error reading CA certificate file: %v", err)
		}

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCertBytes); !ok {
			glog.Fatalf("Error appending CA certificate to pool")
		}

		redisHost, _, err := net.SplitHostPort(hostPort)
		if err != nil {
			glog.Fatalf("Error parsing redis host and port from %s: %v", hostPort, err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs: caCertPool,
			ServerName: redisHost,
			MinVersion: tls.VersionTLS12,
		}
		store.Client = redis.NewClient(&redis.Options{
			Addr:     hostPort,
			Password: password,
			DB:       database,
			TLSConfig: tlsConfig,
		})
	} else {
		store.Client = redis.NewClient(&redis.Options{
			Addr:     hostPort,
			Password: password,
			DB:       database,
		})
	}
	store.loadSuperLargeDirectories(superLargeDirectories)
	return
}
