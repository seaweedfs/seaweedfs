package security

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/spf13/viper"
	"io/ioutil"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func LoadServerTLS(config *viper.Viper, component string) grpc.ServerOption {
	if config == nil {
		return nil
	}

	// load cert/key, ca cert
	cert, err := tls.LoadX509KeyPair(config.GetString(component+".cert"), config.GetString(component+".key"))
	if err != nil {
		glog.Errorf("load cert/key error: %v", err)
		return nil
	}
	caCert, err := ioutil.ReadFile(config.GetString("ca"))
	if err != nil {
		glog.Errorf("read ca cert file error: %v", err)
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

	// load cert/key, cacert
	cert, err := tls.LoadX509KeyPair(config.GetString(component+".cert"), config.GetString(component+".key"))
	if err != nil {
		glog.Errorf("load cert/key error: %v", err)
		return grpc.WithInsecure()
	}
	caCert, err := ioutil.ReadFile(config.GetString("ca"))
	if err != nil {
		glog.Errorf("read ca cert file error: %v", err)
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
