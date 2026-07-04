package client

import (
	"net"
	"time"
)

type HttpClientOpt = func(clientCfg *HTTPClient)

func AddDialContext(httpClient *HTTPClient) {
	dialContext := (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 10 * time.Second,
	}).DialContext

	httpClient.Transport.DialContext = dialContext
	httpClient.Client.Transport = httpClient.Transport
}

// WithResponseHeaderTimeout overrides the transport's default response-header
// timeout. Mainly for tests that need a short deadline against an unresponsive
// peer without mutating the package default.
func WithResponseHeaderTimeout(timeout time.Duration) HttpClientOpt {
	return func(httpClient *HTTPClient) {
		if httpClient.Transport != nil {
			httpClient.Transport.ResponseHeaderTimeout = timeout
		}
	}
}
