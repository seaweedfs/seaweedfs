package util

import (
	"net"
	"strings"
)

// HostKey is the hostname/IP of an "host:port" address (or the string itself
// when it has no port). Compared case-insensitively so worker Hello
// "10.87.0.3:33646" matches a volume replica on "10.87.0.3:8080".
func HostKey(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return strings.ToLower(addr)
	}
	return strings.ToLower(host)
}

// HostKeysFromAddresses returns unique HostKey values, skipping empties.
func HostKeysFromAddresses(addrs []string) []string {
	seen := make(map[string]struct{}, len(addrs))
	var out []string
	for _, a := range addrs {
		h := HostKey(a)
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	return out
}

// AddressIsLocal reports whether addr's host is this process's machine
// (loopback or a configured interface address). Used so an EC worker prefers
// a replica on the same box after replica sync.
func AddressIsLocal(addr string) bool {
	host := HostKey(addr)
	if host == "" {
		return false
	}
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return true
	}
	ifaces, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, ia := range ifaces {
		var got net.IP
		switch v := ia.(type) {
		case *net.IPNet:
			got = v.IP
		case *net.IPAddr:
			got = v.IP
		}
		if got == nil {
			continue
		}
		if strings.EqualFold(got.String(), host) {
			return true
		}
		if ip != nil && got.Equal(ip) {
			return true
		}
	}
	return false
}
