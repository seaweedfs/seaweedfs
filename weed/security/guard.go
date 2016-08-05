package security

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
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
	ipWhiteList   []string
	rootWhiteList []string
	SecretKey Secret

	isActive bool
}

func NewGuard(ipWhiteList []string, rootWhiteList []string, secretKey string) *Guard {
	g := &Guard{ipWhiteList: ipWhiteList, rootWhiteList: rootWhiteList, SecretKey: Secret(secretKey)}
	g.isActive = len(g.ipWhiteList) != 0 || len(g.SecretKey) != 0
	return g
}
func (g *Guard) WhiteList2(f func(w http.ResponseWriter, r *http.Request, b bool)) func(w http.ResponseWriter, r *http.Request, b bool) {
	if !g.isActive {
		//if no security needed, just skip all checkings
		return f
	}
	return func(w http.ResponseWriter, r *http.Request, b bool) {
		if err := g.checkWhiteList(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f(w, r, b)
	}
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
	if len(g.ipWhiteList) == 0 {
		glog.V(0).Info("No whitelist specified for operation")
		return nil
	}

	host, err := GetActualRemoteHost(r)
	if err == nil {
		for _, ip := range g.ipWhiteList {

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
					glog.V(0).Infof("Found %s in CIDR whitelist.", r.RemoteAddr)
					return nil
				}
			}

			//
			// Otherwise we're looking for a literal match.
			//
			if ip == host {
				glog.V(0).Infof("Found %s in whitelist.", r.RemoteAddr)
				return nil
			}
			// ::1 is the same as 127.0.0.1 and localhost
			if host == "::1" && (ip == "127.0.0.1" || ip == "localhost") {
				glog.V(0).Infof("Found %s (localhost) in whitelist.", r.RemoteAddr)
				return nil
			}
		}
	}
	// The root whitelist allows exceptions to the IP whitelist, but only by certain root paths in the request.
	if len(g.rootWhiteList) > 0 {
		pathParts := strings.Split(r.RequestURI, "/")
		if len(pathParts) > 0 {
			requestedRoot := pathParts[1]
			for _, root := range g.rootWhiteList {
				if root == requestedRoot {
					glog.V(0).Infof("Found %s in root whitelist.", requestedRoot)
					return nil
				}
			}
			glog.V(0).Infof("Not in root whitelist: %s", requestedRoot)
		}
	}

	glog.V(0).Infof("Not in whitelist: %s", r.RemoteAddr)
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
