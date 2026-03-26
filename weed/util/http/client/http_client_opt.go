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
