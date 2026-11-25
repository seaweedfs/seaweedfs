package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
	ReadPrefixes    []string `json:"read_prefixes,omitempty"`
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
// the identity used in authorization diagnostics. Returns an empty string
// if the signing key is not configured, the token would not expire, or no
// path grant was supplied.
func GenJwtForFilerScope(signingKey SigningKey, expiresAfter time.Duration, id string, allowedPrefixes, readPrefixes []string) EncodedJwt {
	if len(signingKey) == 0 || expiresAfter <= 0 || len(allowedPrefixes) == 0 && len(readPrefixes) == 0 {
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
	ReadPrefixes    []string `json:"read_prefixes,omitempty"`
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

// DecodeJwt verifies tokenString against signingKey, then against each
// non-empty previousKeys entry, so tokens signed before a key rotation still
// verify while the previous key is configured.
func DecodeJwt(signingKey SigningKey, tokenString EncodedJwt, claims jwt.Claims, previousKeys ...SigningKey) (token *jwt.Token, err error) {
	return decodeJwt(signingKey, previousKeys, tokenString, claims)
}

func decodeJwt(signingKey SigningKey, previousKeys []SigningKey, tokenString EncodedJwt, claims jwt.Claims, options ...jwt.ParserOption) (token *jwt.Token, err error) {
	var key interface{} = []byte(signingKey)
	if keySet := verificationKeySet(signingKey, previousKeys); len(keySet.Keys) > 1 {
		key = keySet
	}
	// check exp, nbf
	return jwt.ParseWithClaims(string(tokenString), claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unknown token method")
		}
		return key, nil
	}, options...)
}

func verificationKeySet(signingKey SigningKey, previousKeys []SigningKey) jwt.VerificationKeySet {
	keySet := jwt.VerificationKeySet{Keys: []jwt.VerificationKey{[]byte(signingKey)}}
	for _, previous := range previousKeys {
		if len(previous) > 0 && string(previous) != string(signingKey) {
			keySet.Keys = append(keySet.Keys, []byte(previous))
		}
	}
	return keySet
}

// DecodeJwtWith verifies a filer token with optional previous-key and clock
// leeway support, using the same verification path as DecodeJwt.
func DecodeJwtWith(signingKey, previousKey SigningKey, tokenString EncodedJwt, claims jwt.Claims, leeway time.Duration) (*jwt.Token, error) {
	return decodeJwt(signingKey, []SigningKey{previousKey}, tokenString, claims, jwt.WithLeeway(leeway))
}

const DefaultAssignTokenLifetime = 24 * time.Hour

// SeaweedAssignClaims is an assignment proof returned with AssignVolume to
// the holder of a scoped filer token. The holder presents it when storing the
// assigned chunk, and any filer with the signing key can verify it without
// keeping assignment state.
type SeaweedAssignClaims struct {
	Fid   string `json:"fid"`
	Count int32  `json:"cnt,omitempty"`
	// A pointer distinguishes the default empty collection from a missing claim.
	Collection *string `json:"collection"`
	jwt.RegisteredClaims
}

// AssignSigningKey derives the signing key for assignment proofs from the
// filer key. Domain separation prevents a proof from being accepted as any
// other filer, admin, or volume credential.
func AssignSigningKey(filerKey SigningKey) SigningKey {
	if len(filerKey) == 0 {
		return nil
	}
	h := hmac.New(sha256.New, []byte(filerKey))
	_, _ = h.Write([]byte("seaweedfs/filer/assign"))
	return SigningKey(h.Sum(nil))
}

// GenJwtForAssign mints an expiring assignment proof for fileID, holder and collection.
// A non-positive lifetime uses the safe default rather than minting a proof
// without an expiry. count covers fileID and the derived fileID_N ids.
func GenJwtForAssign(filerKey SigningKey, lifetime time.Duration, fileID, holder string, count int32, collection string) EncodedJwt {
	key := AssignSigningKey(filerKey)
	if len(key) == 0 || fileID == "" || holder == "" || count < 1 {
		return ""
	}
	if lifetime <= 0 {
		lifetime = DefaultAssignTokenLifetime
	}
	now := time.Now()
	claims := SeaweedAssignClaims{
		Fid:        fileID,
		Count:      count,
		Collection: &collection,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        holder,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(lifetime)),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	encoded, err := t.SignedString([]byte(key))
	if err != nil {
		glog.V(0).Infof("Failed to sign assignment claims %+v: %v", t.Claims, err)
		return ""
	}
	return EncodedJwt(encoded)
}

// VerifyAssignJwt reports whether token is a valid assignment proof for
// fileID, holder and collection under the current or previous filer key. A suffixed file
// id is accepted only when its suffix is the canonical decimal form of an
// index in [1, Count).
func VerifyAssignJwt(filerKey, previousFilerKey SigningKey, token EncodedJwt, fileID, holder string, leeway time.Duration, collection string) bool {
	if (len(filerKey) == 0 && len(previousFilerKey) == 0) || token == "" || fileID == "" || holder == "" {
		return false
	}
	// DecodeJwtWith includes its primary key in the verification set. Never
	// give it an empty primary key, including during a previous-only window.
	if len(filerKey) == 0 {
		filerKey, previousFilerKey = previousFilerKey, nil
	}
	claims := &SeaweedAssignClaims{}
	parsed, err := DecodeJwtWith(AssignSigningKey(filerKey), AssignSigningKey(previousFilerKey), token, claims, leeway)
	if err != nil || parsed == nil || !parsed.Valid || claims.ExpiresAt == nil || claims.Count < 1 || claims.ID != holder || claims.Collection == nil || *claims.Collection != collection {
		return false
	}
	base, err := needle.ParseFileIdFromString(claims.Fid)
	if err != nil || base.String() != claims.Fid {
		return false
	}
	if suffix, ok := strings.CutPrefix(fileID, claims.Fid+"_"); ok {
		n, err := strconv.ParseUint(suffix, 10, 31)
		return err == nil && n >= 1 && strconv.FormatUint(n, 10) == suffix &&
			n < uint64(claims.Count) && n <= ^uint64(0)-uint64(base.Key)
	}
	// Structured FileChunk.Fid values render an allocated suffix as base.Key+N,
	// with the same volume and cookie. Compare that exact range without an
	// overflowing addition; do not accept noncanonical string aliases.
	requested, err := needle.ParseFileIdFromString(fileID)
	return err == nil && requested.String() == fileID && requested.VolumeId == base.VolumeId &&
		requested.Cookie == base.Cookie && requested.Key >= base.Key &&
		uint64(requested.Key-base.Key) < uint64(claims.Count)
}

// FilerJwtProvider signs the credential a filer's HTTP API expects. A nil
// provider means the process-wide jwt.filer_signing configuration applies.
type FilerJwtProvider func(isWrite bool) EncodedJwt

// LoadFilerJwtFromFile reads jwt.filer_signing from a security file the way
// LoadClientTLSFromFile reads the TLS section, honoring the same WEED_
// environment precedence. A nil provider means the file configures no filer
// signing keys and the process-wide configuration applies. A file that sets
// only one access level falls back to the process-wide key for the other.
// A key too short for GODEBUG=fips140=only is an error.
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
	if err := util.CheckFips140HmacKeys([]string{"jwt.filer_signing.key", "jwt.filer_signing.read.key"}, []string{string(signingKey), string(readSigningKey)}); err != nil {
		return nil, fmt.Errorf("security config %s: %v", configFile, err)
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
