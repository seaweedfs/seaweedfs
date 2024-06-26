package master

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"net/http"
	"sync"
)

var (
	clientCfg    *util_http.ClientCfg
	clientCfgErr error

	once        sync.Once
	serviceName = util_http.Master
)

func initClientConfig() {
	once.Do(func() {
		clientCfg, clientCfgErr = NewClientCfg()
	})
	if clientCfgErr != nil {
		glog.Fatalf("Error init client config for `%s`:`%s`", serviceName, clientCfgErr)
	}
}

func NewClientCfg(opts ...util_http.NewClientCfgOpt) (*util_http.ClientCfg, error) {
	return util_http.NewClientCfg(serviceName)
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
