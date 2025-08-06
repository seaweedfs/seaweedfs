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
	if g.isEmptyWhiteList {
		return nil
	}

	host := GetActualRemoteHost(r)

	// Check exact match first (works for both IPs and hostnames)
	if _, ok := g.whiteListIp[host]; ok {
		return nil
	}

	// Check CIDR ranges (only for valid IP addresses)
	remote := net.ParseIP(host)
	if remote != nil {
		for _, cidrnet := range g.whiteListCIDR {
			// If the whitelist entry contains a "/" it
			// is a CIDR range, and we should check the
			if cidrnet.Contains(remote) {
				return nil
			}
		}
	}

	glog.V(0).Infof("Not in whitelist: %s (original RemoteAddr: %s)", host, r.RemoteAddr)
	return fmt.Errorf("Not in whitelist: %s", host)
}

func (g *Guard) UpdateWhiteList(whiteList []string) {
	whiteListIp := make(map[string]struct{})
	whiteListCIDR := make(map[string]*net.IPNet)
	for _, ip := range whiteList {
		if strings.Contains(ip, "/") {
			_, cidrnet, err := net.ParseCIDR(ip)
			if err != nil {
				glog.Errorf("Parse CIDR %s in whitelist failed: %v", ip, err)
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
