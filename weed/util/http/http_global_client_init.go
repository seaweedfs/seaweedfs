package http

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

var (
	globalHttpClient     *util_http_client.HTTPClient
	globalHttpClientOnce sync.Once
)

func NewGlobalHttpClient(opt ...util_http_client.HttpClientOpt) (*util_http_client.HTTPClient, error) {
	return util_http_client.NewHttpClient(util_http_client.Client, opt...)
}

// GetGlobalHttpClient returns the process-wide HTTP client, initializing it on
// first use. Lazy init keeps callers that bypass weed.go's main (in-process
// test harnesses, libraries) from dereferencing a nil client.
func GetGlobalHttpClient() *util_http_client.HTTPClient {
	globalHttpClientOnce.Do(initGlobalHttpClient)
	return globalHttpClient
}

func InitGlobalHttpClient() {
	globalHttpClientOnce.Do(initGlobalHttpClient)
}

func initGlobalHttpClient() {
	client, err := NewGlobalHttpClient()
	if err != nil {
		glog.Fatalf("error init global http client: %v", err)
	}
	globalHttpClient = client
}
