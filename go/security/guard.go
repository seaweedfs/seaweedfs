package security

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/go/glog"
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
	whiteList []string
	SecretKey Secret

	isActive bool
}

func NewGuard(whiteList []string, secretKey string) *Guard {
	g := &Guard{whiteList: whiteList, SecretKey: Secret(secretKey)}
	g.isActive = len(g.whiteList) != 0 || len(g.SecretKey) != 0
	return g
}

func (g *Guard) WhiteList(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	if !g.isActive {
		//if no security needed, just skip all checkings
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

func (g *Guard) Secure(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	if !g.isActive {
		//if no security needed, just skip all checkings
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.checkJwt(w, r); err != nil {
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
	if len(g.whiteList) == 0 {
		return nil
	}

	host, err := GetActualRemoteHost(r)
	if err == nil {
		for _, ip := range g.whiteList {

			// If the whitelist entry contains a "/" it
			// is a CIDR range, and we should check the
			// remote host is within it
			if strings.Contains(ip, "/") {
				_, cidrnet, err := net.ParseCIDR(ip)
				if err != nil {
					panic(err)
				}
				remote := net.ParseIP(host)
				if cidrnet.Contains(remote) {
					return nil
				}
			}

			//
			// Otherwise we're looking for a literal match.
			//
			if ip == host {
				return nil
			}
		}
	}

	glog.V(1).Infof("Not in whitelist: %s", r.RemoteAddr)
	return fmt.Errorf("Not in whitelis: %s", r.RemoteAddr)
}

func (g *Guard) checkJwt(w http.ResponseWriter, r *http.Request) error {
	if g.checkWhiteList(w, r) == nil {
		return nil
	}

	if len(g.SecretKey) == 0 {
		return nil
	}

	tokenStr := GetJwt(r)

	if tokenStr == "" {
		return ErrUnauthorized
	}

	// Verify the token
	token, err := DecodeJwt(g.SecretKey, tokenStr)
	if err != nil {
		glog.V(1).Infof("Token verification error from %s: %v", r.RemoteAddr, err)
		return ErrUnauthorized
	}
	if !token.Valid {
		glog.V(1).Infof("Token invliad from %s: %v", r.RemoteAddr, tokenStr)
		return ErrUnauthorized
	}

	glog.V(1).Infof("No permission from %s", r.RemoteAddr)
	return fmt.Errorf("No write permisson from %s", r.RemoteAddr)
}
