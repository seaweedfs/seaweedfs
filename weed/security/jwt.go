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
	AllowedPrefixes []string `json:"allowed_prefixes,omitempty"`
	AllowedMethods  []string `json:"allowed_methods,omitempty"`
	jwt.RegisteredClaims
}

// SeaweedFilerAdminClaims is presented by callers of the filer's IAM gRPC
// service to prove they are authorised to administer users, access keys, and
// policies. The token is signed with the filer write-signing key
// (jwt.filer_signing.key in security.toml).
//
// Validation is delegated to DecodeJwt below: it enforces HS256 via the
// SigningMethodHMAC type check, and jwt/v5 validates exp/nbf on the embedded
// RegisteredClaims. Extra JSON fields in the payload are silently ignored by
// encoding/json, which is the desired behaviour here (forward-compat).
type SeaweedFilerAdminClaims struct {
	jwt.RegisteredClaims
}

// GenJwtForFilerAdmin mints a Bearer token for the filer IAM gRPC service.
// Returns an empty string if the signing key is not configured.
func GenJwtForFilerAdmin(signingKey SigningKey, expiresAfterSec int) EncodedJwt {
	if len(signingKey) == 0 {
		return ""
	}

	claims := SeaweedFilerAdminClaims{
		RegisteredClaims: jwt.RegisteredClaims{},
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
		RegisteredClaims: jwt.RegisteredClaims{},
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
