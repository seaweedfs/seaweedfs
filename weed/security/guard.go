package security

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	ErrUnauthorized = errors.New("unauthorized token")
)

/*
Guard is to ensure data access security.
There are 2 ways to check access:
 1. white list. It's checking request ip address.
 2. JSON Web Token(JWT) generated from secretKey.
    The jwt can come from:
 1. url parameter jwt=...
 2. request header "Authorization"
 3. cookie with the name "jwt"

The white list is checked first because it is easy.
Then the JWT is checked.

The Guard will also check these claims if provided:
1. "exp" Expiration Time
2. "nbf" Not Before

Generating JWT:
 1. use HS256 to sign
 2. optionally set "exp", "nbf" fields, in Unix time,
    the number of seconds elapsed since January 1, 1970 UTC.

There are two whitelist check entry points with different defaults:
  - IsWhiteListed: allow-all when the whitelist is empty. Kept for HTTP
    compatibility (e.g. Guard.WhiteList middleware on read-mostly routes).
  - IsAdminAuthorized: fail-closed when the whitelist is empty. Used to gate
    destructive admin endpoints so an unconfigured deployment does not
    accept them from arbitrary peers.

Referenced:
https://github.com/pkieltyka/jwtauth/blob/master/jwtauth.go
*/
type Guard struct {
	whiteListIp         map[string]struct{}
	whiteListCIDR       map[string]*net.IPNet
	SigningKey          SigningKey
	ExpiresAfterSec     int
	ReadSigningKey      SigningKey
	ReadExpiresAfterSec int

	isWriteActive    bool
	isEmptyWhiteList bool
}

func NewGuard(whiteList []string, signingKey string, expiresAfterSec int, readSigningKey string, readExpiresAfterSec int) *Guard {
	g := &Guard{
		SigningKey:          SigningKey(signingKey),
		ExpiresAfterSec:     expiresAfterSec,
		ReadSigningKey:      SigningKey(readSigningKey),
		ReadExpiresAfterSec: readExpiresAfterSec,
	}
	g.UpdateWhiteList(whiteList)
	return g
}

func (g *Guard) WhiteList(f http.HandlerFunc) http.HandlerFunc {
	if !g.isWriteActive {
		//if no security needed, just skip all checking
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.checkWhiteList(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f(w, r)
	}
}

func GetActualRemoteHost(r *http.Request) string {
	// For security reasons, only use RemoteAddr to determine the client's IP address.
	// Do not trust headers like X-Forwarded-For, as they can be easily spoofed by clients.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}

	// If SplitHostPort fails, it may be because of a missing port.
	// We try to parse RemoteAddr as a raw host (IP or hostname).
	host = strings.TrimSpace(r.RemoteAddr)
	// It might be an IPv6 address without a port, but with brackets.
	// e.g. "[::1]"
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	// Return the host (can be IP or hostname, just like headers)
	return host
}

func (g *Guard) checkWhiteList(w http.ResponseWriter, r *http.Request) error {
	host := GetActualRemoteHost(r)
	if g.IsWhiteListed(host) {
		return nil
	}
	glog.V(0).Infof("Not in whitelist: %s (original RemoteAddr: %s)", host, r.RemoteAddr)
	return fmt.Errorf("Not in whitelist: %s", host)
}

// IsWhiteListed returns true if the given host IP is allowed by the guard.
// When no whitelist is configured (security inactive), all hosts are allowed.
// This allow-all-when-empty behaviour is kept for HTTP compatibility: many
// non-write HTTP routes wrap themselves with Guard.WhiteList and must remain
// reachable on default deployments. Callers gating destructive endpoints must
// use IsAdminAuthorized instead.
func (g *Guard) IsWhiteListed(host string) bool {
	if !g.isWriteActive {
		return true
	}
	if g.isEmptyWhiteList {
		return true
	}
	if _, ok := g.whiteListIp[host]; ok {
		return true
	}
	remote := net.ParseIP(host)
	if remote != nil {
		for _, cidrnet := range g.whiteListCIDR {
			if cidrnet.Contains(remote) {
				return true
			}
		}
	}
	return false
}

// IsAdminAuthorized is the explicit, fail-closed equivalent of IsWhiteListed
// for destructive admin endpoints. Unlike IsWhiteListed, it returns false
// when no whitelist is configured, so callers must opt in by populating the
// whitelist (CLI -whiteList or guard.white_list in security.toml).
//
// Loopback peers are trusted even when the whitelist is empty: a volume
// server commonly cohabits with the master/filer on a single host (and on
// every integration-test cluster), and an in-process loopback caller is
// equivalent to local-host trust. Off-host callers remain denied.
func (g *Guard) IsAdminAuthorized(host string) bool {
	if g.isEmptyWhiteList {
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return true
		}
		return false
	}
	return g.IsWhiteListed(host)
}

func (g *Guard) UpdateWhiteList(whiteList []string) {
	whiteListIp := make(map[string]struct{})
	whiteListCIDR := make(map[string]*net.IPNet)
	for _, ip := range whiteList {
		if strings.Contains(ip, "/") {
			_, cidrnet, err := net.ParseCIDR(ip)
			if err != nil {
				glog.Errorf("Parse CIDR %s in whitelist failed: %v", ip, err)
				continue
			}
			whiteListCIDR[ip] = cidrnet
		} else {
			whiteListIp[ip] = struct{}{}
		}
	}
	g.isEmptyWhiteList = len(whiteListIp) == 0 && len(whiteListCIDR) == 0
	g.isWriteActive = !g.isEmptyWhiteList || len(g.SigningKey) != 0
	g.whiteListIp = whiteListIp
	g.whiteListCIDR = whiteListCIDR
}
