package client

import (
	"net"
	"sync"
	"time"
)

type HttpClientOpt = func(clientCfg *HTTPClient)

var defaultDialLocalAddress struct {
	sync.RWMutex
	ip net.IP
}

func SetDefaultDialLocalAddress(ip net.IP) {
	defaultDialLocalAddress.Lock()
	defer defaultDialLocalAddress.Unlock()
	if ip == nil || ip.IsUnspecified() {
		defaultDialLocalAddress.ip = nil
		return
	}
	defaultDialLocalAddress.ip = append(net.IP(nil), ip...)
}

func getDefaultDialLocalAddress() net.IP {
	defaultDialLocalAddress.RLock()
	defer defaultDialLocalAddress.RUnlock()
	if defaultDialLocalAddress.ip == nil {
		return nil
	}
	return append(net.IP(nil), defaultDialLocalAddress.ip...)
}

func newDialer(localAddress net.IP) *net.Dialer {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 10 * time.Second,
	}
	if localAddress != nil && !localAddress.IsUnspecified() {
		dialer.LocalAddr = &net.TCPAddr{IP: localAddress}
	}
	return dialer
}

func AddDialContext(httpClient *HTTPClient) {
	dialContext := newDialer(getDefaultDialLocalAddress()).DialContext

	httpClient.Transport.DialContext = dialContext
	httpClient.Client.Transport = httpClient.Transport
}

func AddDialContextWithLocalAddress(localAddress net.IP) HttpClientOpt {
	return func(httpClient *HTTPClient) {
		dialContext := newDialer(localAddress).DialContext

		httpClient.Transport.DialContext = dialContext
		httpClient.Client.Transport = httpClient.Transport
	}
}
