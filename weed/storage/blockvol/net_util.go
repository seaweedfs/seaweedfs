package blockvol

import (
	"net"
	"strconv"
)

// canonicalizeListenerAddr resolves wildcard listener addresses to a routable
// ip:port string using the provided advertised host as the preferred IP.
//
// If the listener bound to a wildcard (":0", "0.0.0.0:port", "[::]:port"),
// the returned address uses advertisedHost instead of the wildcard.
//
// If advertisedHost is empty, falls back to preferredOutboundIP() as a
// best-effort guess. On multi-NIC hosts, the advertised host should always
// be provided to avoid selecting the wrong network interface.
func canonicalizeListenerAddr(addr net.Addr, advertisedHost string) string {
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return addr.String()
	}
	ip := tcpAddr.IP
	if ip != nil && !ip.IsUnspecified() {
		// Already bound to a specific IP — return as-is.
		return net.JoinHostPort(ip.String(), strconv.Itoa(tcpAddr.Port))
	}
	// Wildcard bind — use advertised host or fallback.
	host := advertisedHost
	if host == "" {
		host = PreferredOutboundIP()
	}
	if host == "" {
		// Last resort: return raw address (will be ":port").
		return addr.String()
	}
	return net.JoinHostPort(host, strconv.Itoa(tcpAddr.Port))
}

// preferredOutboundIP returns the machine's preferred outbound IP as a string.
// Uses the standard Go pattern: dial a UDP socket (no data sent), read the
// local address. Returns "" if discovery fails.
//
// This is a fallback — callers should prefer an explicitly configured
// advertised host when available.
// PreferredOutboundIP returns the machine's preferred outbound IP as a string.
func PreferredOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
