package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
)

type EncodedJwt string
type SigningKey []byte

// BearerPrefix is the RFC 6750 Authorization header scheme prefix for bearer
// tokens. Used when constructing "Authorization: Bearer <token>" headers; the
// scheme name itself is matched case-insensitively when parsing (see GetJwt).
const BearerPrefix = "Bearer "

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
	// ReadPrefixes scopes the token to paths it may read but not write. The
	// filer HTTP API honours it for GET and HEAD; the filesystem gRPC API for
	// every read-only call. A token with AllowedPrefixes or ReadPrefixes is a
	// scoped credential (IsScoped); one with neither has full access.
	ReadPrefixes []string `json:"read_prefixes,omitempty"`
	// SessionId is present only on STS session tokens; a token carrying it is
	// an S3 session credential, not a filer API credential.
	SessionId string `json:"sid,omitempty"`
	jwt.RegisteredClaims
}

// IsScoped reports whether the token restricts the paths it may touch.
func (c *SeaweedFilerClaims) IsScoped() bool {
	return c != nil && (len(c.AllowedPrefixes) > 0 || len(c.ReadPrefixes) > 0)
}

// GenJwtForFilerScope mints a filer credential restricted to allowedPrefixes
// (read and write) and readPrefixes (read only). id becomes the "jti" claim:
// the filer keys the chunks it hands out to the holder by it, so a renewed
// token for the same holder must carry the same id. Returns an empty string
// if the signing key is not configured or the token would not expire.
func GenJwtForFilerScope(signingKey SigningKey, expiresAfter time.Duration, id string, allowedPrefixes, readPrefixes []string) EncodedJwt {
	if len(signingKey) == 0 || expiresAfter <= 0 {
		return ""
	}
	claims := SeaweedFilerClaims{
		AllowedPrefixes: allowedPrefixes,
		ReadPrefixes:    readPrefixes,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        id,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(expiresAfter)),
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
	// SessionId is present only on STS session tokens; a token carrying it is
	// an S3 session credential, not a filer admin credential.
	SessionId string `json:"sid,omitempty"`
	// A filer token scoped to paths or methods is not an admin credential.
	AllowedPrefixes []string `json:"allowed_prefixes,omitempty"`
	AllowedMethods  []string `json:"allowed_methods,omitempty"`
	// Read-only path grants also exclude a filer token from the admin plane.
	ReadPrefixes []string `json:"read_prefixes,omitempty"`
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
	return decodeJwt(signingKey, tokenString, claims)
}

func decodeJwt(signingKey SigningKey, tokenString EncodedJwt, claims jwt.Claims, options ...jwt.ParserOption) (token *jwt.Token, err error) {
	// check exp, nbf
	return jwt.ParseWithClaims(string(tokenString), claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unknown token method")
		}
		return []byte(signingKey), nil
	}, options...)
}

// DecodeJwtWith is DecodeJwt with a clock leeway for "exp"/"nbf" and an
// optional previous key: a token whose signature fails under signingKey is
// verified once more under previousKey (jwt.filer_signing.previous_key), so a
// key rotation can keep accepting tokens minted before it for a window.
func DecodeJwtWith(signingKey, previousKey SigningKey, tokenString EncodedJwt, claims jwt.Claims, leeway time.Duration) (token *jwt.Token, err error) {
	token, err = decodeJwt(signingKey, tokenString, claims, jwt.WithLeeway(leeway))
	if err != nil && len(previousKey) > 0 && errors.Is(err, jwt.ErrTokenSignatureInvalid) {
		token, err = decodeJwt(previousKey, tokenString, claims, jwt.WithLeeway(leeway))
	}
	return token, err
}

// SeaweedAssignClaims is the assignment token a filer returns with
// AssignVolume to the holder of a scoped filer token (AssignVolumeResponse
// assign_auth): it states that file id Fid was assigned to the holder named
// by "jti". The holder sends it back (chunk_auth) when it stores the chunk,
// and any filer that shares the signing key verifies it without remembering
// the assignment. It is signed with AssignSigningKey, derived from the filer
// key, so it is never accepted as a filer, admin or volume credential.
type SeaweedAssignClaims struct {
	Fid string `json:"fid"`
	// Count is the number of file ids the assignment issued: the base Fid plus
	// Fid_1 .. Fid_{Count-1}, which the volume server derives from the base. A
	// suffixed id is covered only when its index is within Count, so a proof
	// cannot cover ids the allocation never issued.
	Count int32 `json:"cnt,omitempty"`
	jwt.RegisteredClaims
}

// AssignSigningKey derives the key assignment tokens are signed with from
// jwt.filer_signing.key. Returns nil when no filer key is configured.
func AssignSigningKey(filerKey SigningKey) SigningKey {
	if len(filerKey) == 0 {
		return nil
	}
	h := hmac.New(sha256.New, []byte(filerKey))
	h.Write([]byte("seaweedfs/filer/assign"))
	return SigningKey(h.Sum(nil))
}

// GenJwtForAssign mints the assignment token of fileId for holder. A
// lifetime of zero or less mints a token without "exp": it then lives as long
// as the holder's own scoped tokens are renewed, since the filer accepts it
// only together with a valid token of the same holder.
func GenJwtForAssign(filerKey SigningKey, lifetime time.Duration, fileId, holder string, count int32) EncodedJwt {
	key := AssignSigningKey(filerKey)
	if len(key) == 0 || fileId == "" || holder == "" {
		return ""
	}
	if count < 1 {
		count = 1
	}
	claims := SeaweedAssignClaims{
		Fid:              fileId,
		Count:            count,
		RegisteredClaims: jwt.RegisteredClaims{ID: holder, IssuedAt: jwt.NewNumericDate(time.Now())},
	}
	if lifetime > 0 {
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(lifetime))
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	encoded, e := t.SignedString([]byte(key))
	if e != nil {
		glog.V(0).Infof("Failed to sign assignment claims %+v: %v", t.Claims, e)
		return ""
	}
	return EncodedJwt(encoded)
}

// VerifyAssignJwt reports whether token is a valid assignment token of fileId
// for holder under filerKey or previousFilerKey, within leeway of its expiry.
// A multi-count assignment ("fid_1", "fid_2", ...) is covered by the token of
// its base file id, as the volume server accepts those ids.
func VerifyAssignJwt(filerKey, previousFilerKey SigningKey, token EncodedJwt, fileId, holder string, leeway time.Duration) bool {
	if token == "" || fileId == "" || holder == "" {
		return false
	}
	claims := &SeaweedAssignClaims{}
	parsed, err := DecodeJwtWith(AssignSigningKey(filerKey), AssignSigningKey(previousFilerKey), token, claims, leeway)
	if err != nil || parsed == nil || !parsed.Valid {
		return false
	}
	if claims.ID != holder {
		return false
	}
	// The base id the assignment issued is always covered.
	if claims.Fid == fileId {
		return true
	}
	// A multi-count assignment of Count ids covers base, base_1 .. base_{Count-1}.
	// Accept a suffixed id only when its base matches and its index is within the
	// allocated count, so a single-id proof cannot reference ids never issued.
	suffix, ok := strings.CutPrefix(fileId, claims.Fid+"_")
	if !ok {
		return false
	}
	n, convErr := strconv.Atoi(suffix)
	return convErr == nil && n >= 1 && int32(n) < claims.Count
}

// FilerJwtProvider signs the credential a filer's HTTP API expects. A nil
// provider means the process-wide jwt.filer_signing configuration applies.
type FilerJwtProvider func(isWrite bool) EncodedJwt

// LoadFilerJwtFromFile reads jwt.filer_signing from a security file the way
// LoadClientTLSFromFile reads the TLS section, honoring the same WEED_
// environment precedence. A nil provider means the file configures no filer
// signing keys and the process-wide configuration applies. A file that sets
// only one access level falls back to the process-wide key for the other.
func LoadFilerJwtFromFile(configFile string) (FilerJwtProvider, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	v.AutomaticEnv()
	v.SetEnvPrefix("weed")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read security config %s: %v", configFile, err)
	}

	signingKey := SigningKey(v.GetString("jwt.filer_signing.key"))
	readSigningKey := SigningKey(v.GetString("jwt.filer_signing.read.key"))
	if len(signingKey) == 0 && len(readSigningKey) == 0 {
		return nil, nil
	}
	signingKeyExpires := v.GetInt("jwt.filer_signing.expires_after_seconds")
	readSigningKeyExpires := v.GetInt("jwt.filer_signing.read.expires_after_seconds")
	if len(signingKey) == 0 || len(readSigningKey) == 0 {
		gv := util.GetViper()
		if len(signingKey) == 0 {
			signingKey = SigningKey(gv.GetString("jwt.filer_signing.key"))
			signingKeyExpires = gv.GetInt("jwt.filer_signing.expires_after_seconds")
		}
		if len(readSigningKey) == 0 {
			readSigningKey = SigningKey(gv.GetString("jwt.filer_signing.read.key"))
			readSigningKeyExpires = gv.GetInt("jwt.filer_signing.read.expires_after_seconds")
		}
	}
	if signingKeyExpires < 0 || readSigningKeyExpires < 0 {
		return nil, fmt.Errorf("jwt.filer_signing lifetimes must not be negative")
	}
	if signingKeyExpires == 0 {
		signingKeyExpires = 10
	}
	if readSigningKeyExpires == 0 {
		readSigningKeyExpires = 60
	}
	return func(isWrite bool) EncodedJwt {
		if isWrite {
			return GenJwtForFilerServer(signingKey, signingKeyExpires)
		}
		return GenJwtForFilerServer(readSigningKey, readSigningKeyExpires)
	}, nil
}
