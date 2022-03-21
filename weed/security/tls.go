package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Authenticator struct {
	AllowedWildcardDomain string
	AllowedCommonNames    map[string]bool
}

func LoadServerTLS(config *util.ViperProxy, component string) (grpc.ServerOption, grpc.ServerOption) {
	if config == nil {
		return nil, nil
	}

	// load cert/key, ca cert
	cert, err := tls.LoadX509KeyPair(config.GetString(component+".cert"), config.GetString(component+".key"))
	if err != nil {
		glog.V(1).Infof("load cert: %s / key: %s error: %v",
			config.GetString(component+".cert"),
			config.GetString(component+".key"),
			err)
		return nil, nil
	}
	caCert, err := os.ReadFile(config.GetString("grpc.ca"))
	if err != nil {
		glog.V(1).Infof("read ca cert file %s error: %v", config.GetString("grpc.ca"), err)
		return nil, nil
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	ta := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})

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
		return grpc.Creds(ta), grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(auther.Authenticate))
	}
	return grpc.Creds(ta), nil
}

func LoadClientTLS(config *util.ViperProxy, component string) grpc.DialOption {
	if config == nil {
		return grpc.WithInsecure()
	}

	certFileName, keyFileName, caFileName := config.GetString(component+".cert"), config.GetString(component+".key"), config.GetString("grpc.ca")
	if certFileName == "" || keyFileName == "" || caFileName == "" {
		return grpc.WithInsecure()
	}

	// load cert/key, cacert
	cert, err := tls.LoadX509KeyPair(certFileName, keyFileName)
	if err != nil {
		glog.V(1).Infof("load cert/key error: %v", err)
		return grpc.WithInsecure()
	}
	caCert, err := os.ReadFile(caFileName)
	if err != nil {
		glog.V(1).Infof("read ca cert file error: %v", err)
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

func LoadClientTLSHTTP(clientCertFile string) *tls.Config {
	clientCerts, err := ioutil.ReadFile(clientCertFile)
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

func (a Authenticator) Authenticate(ctx context.Context) (newCtx context.Context, err error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "no peer found")
	}

	tlsAuth, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "unexpected peer transport credentials")
	}
	if len(tlsAuth.State.VerifiedChains) == 0 || len(tlsAuth.State.VerifiedChains[0]) == 0 {
		return ctx, status.Error(codes.Unauthenticated, "could not verify peer certificate")
	}

	commonName := tlsAuth.State.VerifiedChains[0][0].Subject.CommonName
	if a.AllowedWildcardDomain != "" && strings.HasSuffix(commonName, a.AllowedWildcardDomain) {
		return ctx, nil
	}
	if _, ok := a.AllowedCommonNames[commonName]; ok {
		return ctx, nil
	}

	return ctx, status.Errorf(codes.Unauthenticated, "invalid subject common name: %s", commonName)
}
