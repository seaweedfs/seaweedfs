package http

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

var (
	globalHttpClient *util_http_client.HTTPClient
)

func NewGlobalHttpClient(opt ...util_http_client.HttpClientOpt) (*util_http_client.HTTPClient, error) {
	return util_http_client.NewHttpClient(util_http_client.Client, opt...)
}

func GetGlobalHttpClient() *util_http_client.HTTPClient {
	return globalHttpClient
}

func InitGlobalHttpClient() {
	var err error

	globalHttpClient, err = NewGlobalHttpClient()
	if err != nil {
		glog.Fatalf("error init global http client: %v", err)
	}
}

func ReInitGlobalHttpClient(opt ...util_http_client.HttpClientOpt) {
	var err error

	globalHttpClient, err = NewGlobalHttpClient(opt...)
	if err != nil {
		glog.Fatalf("error reinit global http client: %v", err)
	}
}
