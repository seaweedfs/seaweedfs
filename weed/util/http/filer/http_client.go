package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"net/http"
	"sync"
)

var (
	clientCfg   *util_http.ClientCfg
	once        sync.Once
	serviceName = util_http.Filer
)

func initClientConfig() {
	var err error
	once.Do(func() {
		clientCfg, err = util_http.NewClientCfg(serviceName)
	})
	if err != nil {
		glog.Fatalf("Error init client config for `%s`:`%s`", serviceName, err)
	}
}

func GetClientCfg() *util_http.ClientCfg {
	initClientConfig()
	return clientCfg
}

func GetClient() *http.Client {
	initClientConfig()
	return clientCfg.Client
}

func GetClientTransport() *http.Transport {
	initClientConfig()
	return clientCfg.Transport
}
