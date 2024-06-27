package http

import (
	"net"
	"time"
)

type HttpClientOpt = func(clientCfg *HttpClient)

func AddDialContext(httpClient *HttpClient) {
	dialContext := (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 10 * time.Second,
	}).DialContext

	httpClient.Transport.DialContext = dialContext
	httpClient.Client.Transport = httpClient.Transport
}
