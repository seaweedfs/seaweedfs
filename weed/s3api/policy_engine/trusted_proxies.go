package policy_engine

import (
	"net"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type TrustedProxies struct {
	ips   map[string]struct{}
	cidrs map[string]*net.IPNet
}

func NewTrustedProxies(whiteList []string) *TrustedProxies {
	tp := &TrustedProxies{
		ips:   make(map[string]struct{}),
		cidrs: make(map[string]*net.IPNet),
	}
	for _, entry := range whiteList {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if strings.Contains(entry, "/") {
			_, cidrnet, err := net.ParseCIDR(entry)
			if err != nil {
				glog.Errorf("Parse CIDR %s in s3 trusted_proxies failed: %v", entry, err)
				continue
			}
			tp.cidrs[entry] = cidrnet
		} else {
			ip := net.ParseIP(entry)
			if ip == nil {
				glog.Errorf("Parse IP %s in s3 trusted_proxies failed", entry)
				continue
			}
			tp.ips[ip.String()] = struct{}{}
		}
	}
	return tp
}

func (tp *TrustedProxies) IsTrusted(ipStr string) bool {
	if tp == nil {
		return false
	}
	if _, ok := tp.ips[ipStr]; ok {
		return true
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, cidrnet := range tp.cidrs {
		if cidrnet.Contains(ip) {
			return true
		}
	}
	return false
}

// ExtractSourceIP returns the client IP for aws:SourceIp condition evaluation.
// The direct TCP peer is used unless it is in the trusted proxy allowlist, in
// which case X-Forwarded-For (right-to-left, skipping trusted hops) then
// X-Real-Ip are honored.
func (tp *TrustedProxies) ExtractSourceIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	remoteAddr := strings.TrimSpace(r.RemoteAddr)
	if remoteAddr == "" {
		return ""
	}
	if remoteAddr == "@" {
		return remoteAddr
	}
	host := remoteAddr
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = h
	}
	remoteIP := net.ParseIP(host)
	if remoteIP == nil {
		return ""
	}
	if tp.IsTrusted(host) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			entries := strings.Split(xff, ",")
			malformed := false
			for i := len(entries) - 1; i >= 0; i-- {
				candidate := strings.TrimSpace(entries[i])
				if candidate == "" {
					continue
				}
				ip := net.ParseIP(candidate)
				if ip == nil {
					malformed = true
					break
				}
				if tp.IsTrusted(ip.String()) {
					continue
				}
				return ip.String()
			}
			if !malformed {
				for _, candidate := range entries {
					candidate = strings.TrimSpace(candidate)
					if ip := net.ParseIP(candidate); ip != nil {
						return ip.String()
					}
				}
			}
		}
		if xRealIP := strings.TrimSpace(r.Header.Get("X-Real-Ip")); xRealIP != "" {
			if ip := net.ParseIP(xRealIP); ip != nil {
				return ip.String()
			}
		}
	}
	return remoteIP.String()
}
