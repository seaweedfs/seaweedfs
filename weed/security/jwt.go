package security

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	jwt "github.com/dgrijalva/jwt-go"
)

type EncodedJwt string
type SigningKey []byte

type SeaweedFileIdClaims struct {
	Fid string `json:"fid"`
	jwt.StandardClaims
}

func GenJwt(signingKey SigningKey, fileId string) EncodedJwt {
	if len(signingKey) == 0 {
		return ""
	}

	claims := SeaweedFileIdClaims{
		fileId,
		jwt.StandardClaims{
			ExpiresAt: time.Now().Add(time.Second * 10).Unix(),
		},
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

func DecodeJwt(signingKey SigningKey, tokenString EncodedJwt) (token *jwt.Token, err error) {
	// check exp, nbf
	return jwt.ParseWithClaims(string(tokenString), &SeaweedFileIdClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unknown token method")
		}
		return []byte(signingKey), nil
	})
}
