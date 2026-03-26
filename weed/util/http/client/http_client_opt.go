package client

import (
	"crypto/tls"
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

func WithInsecureSkipVerify(httpClient *HTTPClient) {
	if httpClient.Transport.TLSClientConfig == nil {
		httpClient.Transport.TLSClientConfig = &tls.Config{}
	}
	httpClient.Transport.TLSClientConfig.InsecureSkipVerify = true
}
