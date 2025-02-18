package security

import (
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"net"
	"net/http"
	"strings"
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

func GetActualRemoteHost(r *http.Request) (host string, err error) {
	host = r.Header.Get("HTTP_X_FORWARDED_FOR")
	if host == "" {
		host = r.Header.Get("X-FORWARDED-FOR")
	}
	if strings.Contains(host, ",") {
		host = host[0:strings.Index(host, ",")]
	}
	if host == "" {
		host, _, err = net.SplitHostPort(r.RemoteAddr)
	}
	return
}

func (g *Guard) checkWhiteList(w http.ResponseWriter, r *http.Request) error {
	if g.isEmptyWhiteList {
		return nil
	}

	host, err := GetActualRemoteHost(r)
	if err != nil {
		return fmt.Errorf("get actual remote host %s in checkWhiteList failed: %v", r.RemoteAddr, err)
	}

	if _, ok := g.whiteListIp[host]; ok {
		return nil
	}

	for _, cidrnet := range g.whiteListCIDR {
		// If the whitelist entry contains a "/" it
		// is a CIDR range, and we should check the
		remote := net.ParseIP(host)
		if cidrnet.Contains(remote) {
			return nil
		}
	}

	glog.V(0).Infof("Not in whitelist: %s", r.RemoteAddr)
	return fmt.Errorf("Not in whitelist: %s", r.RemoteAddr)
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
