package security

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/golang-jwt/jwt"
)

type EncodedJwt string
type SigningKey []byte

// SeaweedFileIdClaims is created by Master server(s) and consumed by Volume server(s),
// restricting the access this JWT allows to only a single file.
type SeaweedFileIdClaims struct {
	Fid string `json:"fid"`
	jwt.StandardClaims
}

// SeaweedFilerClaims is created e.g. by S3 proxy server and consumed by Filer server.
// Right now, it only contains the standard claims; but this might be extended later
// for more fine-grained permissions.
type SeaweedFilerClaims struct {
	jwt.StandardClaims
}

func GenJwtForVolumeServer(signingKey SigningKey, expiresAfterSec int, fileId string) EncodedJwt {
	if len(signingKey) == 0 {
		return ""
	}

	claims := SeaweedFileIdClaims{
		fileId,
		jwt.StandardClaims{},
	}
	if expiresAfterSec > 0 {
		claims.ExpiresAt = time.Now().Add(time.Second * time.Duration(expiresAfterSec)).Unix()
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	encoded, e := t.SignedString([]byte(signingKey))
	if e != nil {
		glog.V(0).Infof("Failed to sign claims %+v: %v", t.Claims, e)
		return ""
	}
	return EncodedJwt(encoded)
}

// GenJwtForFilerServer creates a JSON-web-token for using the authenticated Filer API. Used f.e. inside
// the S3 API
func GenJwtForFilerServer(signingKey SigningKey, expiresAfterSec int) EncodedJwt {
	if len(signingKey) == 0 {
		return ""
	}

	claims := SeaweedFilerClaims{
		jwt.StandardClaims{},
	}
	if expiresAfterSec > 0 {
		claims.ExpiresAt = time.Now().Add(time.Second * time.Duration(expiresAfterSec)).Unix()
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	encoded, e := t.SignedString([]byte(signingKey))
	if e != nil {
		glog.V(0).Infof("Failed to sign claims %+v: %v", t.Claims, e)
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

	return EncodedJwt(tokenStr)
}

func DecodeJwt(signingKey SigningKey, tokenString EncodedJwt, claims jwt.Claims) (token *jwt.Token, err error) {
	// check exp, nbf
	return jwt.ParseWithClaims(string(tokenString), claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unknown token method")
		}
		return []byte(signingKey), nil
	})
}
