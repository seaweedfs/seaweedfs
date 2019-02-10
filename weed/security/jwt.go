package security

import (
	"net/http"
	"strings"

	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	jwt "github.com/dgrijalva/jwt-go"
)

type EncodedJwt string
type SigningKey string

func GenJwt(signingKey SigningKey, fileId string) EncodedJwt {
	if signingKey == "" {
		return ""
	}

	t := jwt.New(jwt.GetSigningMethod("HS256"))
	t.Claims = &jwt.StandardClaims{
		ExpiresAt: time.Now().Add(time.Second * 10).Unix(),
		Subject:   fileId,
	}
	encoded, e := t.SignedString(signingKey)
	if e != nil {
		glog.V(0).Infof("Failed to sign claims: %v", t.Claims)
		return ""
	}
	return EncodedJwt(encoded)
}

func GetJwt(r *http.Request) EncodedJwt {

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

	return EncodedJwt(tokenStr)
}

func EncodeJwt(signingKey SigningKey, claims *jwt.StandardClaims) (EncodedJwt, error) {
	if signingKey == "" {
		return "", nil
	}

	t := jwt.New(jwt.GetSigningMethod("HS256"))
	t.Claims = claims
	encoded, e := t.SignedString(signingKey)
	return EncodedJwt(encoded), e
}

func DecodeJwt(signingKey SigningKey, tokenString EncodedJwt) (token *jwt.Token, err error) {
	// check exp, nbf
	return jwt.Parse(string(tokenString), func(token *jwt.Token) (interface{}, error) {
		return signingKey, nil
	})
}
