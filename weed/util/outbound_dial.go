package util

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// outboundLocalAddr is the optional source address that outbound TCP
// connections initiated by this process bind to. It mirrors the -ip.bind
// setting so connections this process opens leave from the configured
// address instead of one the OS picks arbitrarily, which may not even be able
// to reach the target. A nil value keeps the historical behavior of letting
// the OS choose the source address.
var (
	outboundLocalAddr    atomic.Pointer[net.TCPAddr]
	outboundLocalAddrSet atomic.Bool
)

// SetOutboundLocalIP records ip as the source address for outbound TCP
// connections. An empty, wildcard (0.0.0.0 / ::), or unparseable value leaves
// the source address to the OS.
//
// The first call wins. `weed server` applies the top-level -ip.bind before its
// embedded master/volume/filer/s3/... start, so a component's own (possibly
// different) bind setting can't later clobber the process-wide source address
// the others already dial from.
func SetOutboundLocalIP(ip string) {
	if !outboundLocalAddrSet.CompareAndSwap(false, true) {
		return
	}
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.IsUnspecified() {
		return
	}
	outboundLocalAddr.Store(&net.TCPAddr{IP: parsed})
}

// OutboundLocalAddr returns the configured source address for outbound TCP
// connections, or nil if none is configured.
func OutboundLocalAddr() *net.TCPAddr {
	return outboundLocalAddr.Load()
}

// OutboundDialContext dials network/address with the standard library's
// default timeouts, binding the local (source) address to the configured
// -ip.bind value for non-loopback tcp targets. Loopback targets and non-tcp
// networks (e.g. unix sockets) keep the OS-chosen source address: a routable
// bind IP cannot originate a packet on the loopback interface.
func OutboundDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d := net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if localAddr := outboundLocalAddrForDial(network, address); localAddr != nil {
		d.LocalAddr = localAddr
	}
	return d.DialContext(ctx, network, address)
}

// outboundLocalAddrForDial returns the source address to bind for a dial to
// network/address, or nil to leave the source address to the OS.
func outboundLocalAddrForDial(network, address string) *net.TCPAddr {
	localAddr := OutboundLocalAddr()
	if localAddr == nil || !strings.HasPrefix(network, "tcp") || isLoopbackHost(address) {
		return nil
	}
	// A source address can only originate a connection to a target of the same
	// IP family. When the target is a literal IP of the other family, leave the
	// source to the OS rather than forcing a dial that is bound to fail.
	if host, _, err := net.SplitHostPort(address); err == nil {
		if targetIP := net.ParseIP(host); targetIP != nil && isIPv4(targetIP) != isIPv4(localAddr.IP) {
			return nil
		}
	}
	return localAddr
}

// isIPv4 reports whether ip is an IPv4 (or IPv4-mapped) address.
func isIPv4(ip net.IP) bool {
	return ip.To4() != nil
}

// isLoopbackHost reports whether the host part of a "host:port" (or bare host)
// address refers to the loopback interface.
func isLoopbackHost(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}
