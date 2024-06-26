package http

import (
	"net"
	"time"
)

type NewClientCfgOpt = func(clientCfg *ClientCfg)

func AddDialContext(clientCfg *ClientCfg) {
	dialContext := (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 10 * time.Second,
	}).DialContext

	clientCfg.Transport.DialContext = dialContext
	clientCfg.Client.Transport = clientCfg.Transport
}
