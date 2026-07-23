package nfs

import (
	"fmt"
	"net"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type clientAuthorizer struct {
	exact   map[string]struct{}
	cidrs   map[string]*net.IPNet
	enabled bool
}

func newClientAuthorizer(allowed []string) (*clientAuthorizer, error) {
	authorizer := &clientAuthorizer{
		exact: make(map[string]struct{}),
		cidrs: make(map[string]*net.IPNet),
	}

	for _, raw := range allowed {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		if strings.Contains(entry, "/") {
			_, network, err := net.ParseCIDR(entry)
			if err != nil {
				return nil, fmt.Errorf("parse allowed NFS client %q: %w", entry, err)
			}
			authorizer.cidrs[entry] = network
			authorizer.enabled = true
			continue
		}

		if ip := normalizeClientIP(entry); ip != nil {
			authorizer.exact[ip.String()] = struct{}{}
			authorizer.enabled = true
			continue
		}

		ips, err := net.LookupIP(entry)
		if err != nil {
			return nil, fmt.Errorf("resolve allowed NFS client %q: %w", entry, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("resolve allowed NFS client %q: no addresses", entry)
		}
		authorizer.exact[entry] = struct{}{}
		for _, ip := range ips {
			if ip == nil {
				continue
			}
			authorizer.exact[ip.String()] = struct{}{}
		}
		authorizer.enabled = true
	}

	return authorizer, nil
}

func (a *clientAuthorizer) isAllowedConn(conn net.Conn) bool {
	if conn == nil {
		return true
	}
	return a.isAllowedAddr(conn.RemoteAddr())
}

func (a *clientAuthorizer) isAllowedAddr(addr net.Addr) bool {
	if a == nil || !a.enabled {
		return true
	}
	if addr == nil {
		return false
	}

	host := remoteHost(addr.String())
	if host == "" {
		return false
	}
	if _, found := a.exact[host]; found {
		return true
	}

	ip := normalizeClientIP(host)
	if ip == nil {
		return false
	}
	if _, found := a.exact[ip.String()]; found {
		return true
	}
	for _, network := range a.cidrs {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func remoteHost(remote string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remote))
	if err == nil {
		return host
	}

	host = strings.TrimSpace(remote)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	return host
}

func normalizeClientIP(host string) net.IP {
	host = strings.TrimSpace(host)
	if zoneIndex := strings.LastIndex(host, "%"); zoneIndex >= 0 {
		host = host[:zoneIndex]
	}
	return net.ParseIP(host)
}

type allowlistListener struct {
	net.Listener
	authorizer *clientAuthorizer
}

func (l *allowlistListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.authorizer == nil || l.authorizer.isAllowedConn(conn) {
			return conn, nil
		}
		glog.V(0).Infof("reject unauthorized nfs client %s", conn.RemoteAddr())
		_ = conn.Close()
	}
}
