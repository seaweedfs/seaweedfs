package security

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/spf13/viper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/chrislusf/seaweedfs/weed/util/log"
)

func LoadServerTLS(config *viper.Viper, component string) grpc.ServerOption {
	if config == nil {
		return nil
	}

	// load cert/key, ca cert
	cert, err := tls.LoadX509KeyPair(config.GetString(component+".cert"), config.GetString(component+".key"))
	if err != nil {
		log.Debugf("load cert/key error: %v", err)
		return nil
	}
	caCert, err := ioutil.ReadFile(config.GetString(component + ".ca"))
	if err != nil {
		log.Debugf("read ca cert file error: %v", err)
		return nil
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	ta := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})

	return grpc.Creds(ta)
}

func LoadClientTLS(config *viper.Viper, component string) grpc.DialOption {
	if config == nil {
		return grpc.WithInsecure()
	}

	certFileName, keyFileName, caFileName := config.GetString(component+".cert"), config.GetString(component+".key"), config.GetString(component+".ca")
	if certFileName == "" || keyFileName == "" || caFileName == "" {
		return grpc.WithInsecure()
	}

	// load cert/key, cacert
	cert, err := tls.LoadX509KeyPair(certFileName, keyFileName)
	if err != nil {
		log.Debugf("load cert/key error: %v", err)
		return grpc.WithInsecure()
	}
	caCert, err := ioutil.ReadFile(caFileName)
	if err != nil {
		log.Debugf("read ca cert file error: %v", err)
		return grpc.WithInsecure()
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	ta := credentials.NewTLS(&tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	})
	return grpc.WithTransportCredentials(ta)
}
