package security

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type EncodedJwt string
type SigningKey []byte

// SeaweedFileIdClaims is created by Master server(s) and consumed by Volume server(s),
// restricting the access this JWT allows to only a single file.
type SeaweedFileIdClaims struct {
	Fid string `json:"fid"`
	jwt.RegisteredClaims
}

// SeaweedFilerClaims is created e.g. by S3 proxy server and consumed by Filer server.
// Right now, it only contains the standard claims; but this might be extended later
// for more fine-grained permissions.
type SeaweedFilerClaims struct {
	jwt.RegisteredClaims
}

func GenJwtForVolumeServer(signingKey SigningKey, expiresAfterSec int, fileId string) EncodedJwt {
	if len(signingKey) == 0 {
		return ""
	}

	claims := SeaweedFileIdClaims{
		fileId,
		jwt.RegisteredClaims{},
	}
	if expiresAfterSec > 0 {
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Second * time.Duration(expiresAfterSec)))
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
		jwt.RegisteredClaims{},
	}
	if expiresAfterSec > 0 {
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Second * time.Duration(expiresAfterSec)))
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

	// Get token from http only cookie
	if tokenStr == "" {
		token, err := r.Cookie("AT")
		if err == nil {
			tokenStr = token.Value
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
