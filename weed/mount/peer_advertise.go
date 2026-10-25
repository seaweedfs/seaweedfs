package mount

import (
	"fmt"
	"net"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ResolvePeerAdvertiseAddr returns the host:port this mount should
// register with the filer and announce to peers. It handles three
// common cases:
//
//  1. The operator set -peer.advertise=host:port explicitly — use it.
//  2. -peer.listen contains a specific bind host — use that host with
//     the bind port (e.g. "10.0.0.5:18080").
//  3. -peer.listen is a wildcard bind (":18080", "0.0.0.0:18080", or
//     "[::]:18080") — combine util.DetectedHostAddress() with the port.
//
// Returns an error if the listen string is unparseable or if case (3)
// hits and auto-detection turns up nothing. We deliberately do NOT fall
// back to "localhost": an advertised loopback gets registered with the
// filer and hands other mounts a useless address to dial (their own
// loopback). Better to fail loudly so the operator sets -peer.advertise.
func ResolvePeerAdvertiseAddr(listen, advertise string) (string, error) {
	if advertise != "" {
		return advertise, nil
	}
	if listen == "" {
		return "", fmt.Errorf("peer listen address is empty")
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("parse -peer.listen %q: %w", listen, err)
	}
	if !isWildcardHost(host) {
		return net.JoinHostPort(host, port), nil
	}
	detected := util.DetectedHostAddress()
	if detected == "" {
		return "", fmt.Errorf("cannot auto-detect host for wildcard -peer.listen %q; set -peer.advertise=host:port explicitly", listen)
	}
	return net.JoinHostPort(detected, port), nil
}

func isWildcardHost(h string) bool {
	switch h {
	case "", "0.0.0.0", "::", "[::]":
		return true
	}
	return false
}
