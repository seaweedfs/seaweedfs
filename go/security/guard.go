package security

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/dgrijalva/jwt-go"
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
	secretKey string

	isActive bool
}

func NewGuard(whiteList []string, secretKey string) *Guard {
	g := &Guard{whiteList: whiteList, secretKey: secretKey}
	g.isActive = len(g.whiteList) != 0 || len(g.secretKey) != 0
	return g
}

func (g *Guard) Secure(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	if !g.isActive {
		//if no security needed, just skip all checkings
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.doCheck(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f(w, r)
	}
}

func (g *Guard) NewToken() (tokenString string, err error) {
	m := make(map[string]interface{})
	m["exp"] = time.Now().Unix() + 10
	return g.Encode(m)
}

func (g *Guard) Encode(claims map[string]interface{}) (tokenString string, err error) {
	if !g.isActive {
		return "", nil
	}

	t := jwt.New(jwt.GetSigningMethod("HS256"))
	t.Claims = claims
	return t.SignedString(g.secretKey)
}

func (g *Guard) Decode(tokenString string) (token *jwt.Token, err error) {
	return jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return g.secretKey, nil
	})
}

func (g *Guard) doCheck(w http.ResponseWriter, r *http.Request) error {
	if len(g.whiteList) != 0 {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			for _, ip := range g.whiteList {
				if ip == host {
					return nil
				}
			}
		}
	}

	if len(g.secretKey) != 0 {

		// Get token from query params
		tokenStr := r.URL.Query().Get("jwt")

		// Get token from authorization header
		if tokenStr == "" {
			bearer := r.Header.Get("Authorization")
			if len(bearer) > 7 && strings.ToUpper(bearer[0:6]) == "BEARER" {
				tokenStr = bearer[7:]
			}
		}

		// Get token from cookie
		if tokenStr == "" {
			cookie, err := r.Cookie("jwt")
			if err == nil {
				tokenStr = cookie.Value
			}
		}

		if tokenStr == "" {
			return ErrUnauthorized
		}

		// Verify the token
		token, err := g.Decode(tokenStr)
		if err != nil {
			glog.V(1).Infof("Token verification error from %s: %v", r.RemoteAddr, err)
			return ErrUnauthorized
		}
		if !token.Valid {
			glog.V(1).Infof("Token invliad from %s: %v", r.RemoteAddr, tokenStr)
			return ErrUnauthorized
		}

	}

	glog.V(1).Infof("No permission from %s", r.RemoteAddr)
	return fmt.Errorf("No write permisson from %s", r.RemoteAddr)
}
