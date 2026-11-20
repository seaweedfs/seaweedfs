package security

import (
	"errors"
	jwt "github.com/golang-jwt/jwt/v5"
	"time"
)

// IsExpiredVolumeJwt reports a signed, FID-bound credential whose only failure
// is expiration. It never authorizes a request; it classifies an existing denial
// so an assignment-owning caller can obtain a fresh credential with a new FID.
func IsExpiredVolumeJwt(key SigningKey, encoded EncodedJwt, expectedFid string, previousKeys ...SigningKey) bool {
	return isExpiredVolumeJwtAt(key, encoded, expectedFid, time.Now(), previousKeys...)
}

func isExpiredVolumeJwtAt(key SigningKey, encoded EncodedJwt, expectedFid string, now time.Time, previousKeys ...SigningKey) bool {
	if len(key) == 0 || encoded == "" {
		return false
	}
	claims := &SeaweedFileIdClaims{}
	parsed, err := decodeJwt(key, previousKeys, encoded, claims, jwt.WithTimeFunc(func() time.Time { return now }))
	// ParseWithClaims verifies the HMAC signature before validating any claims.
	if parsed == nil || !errors.Is(err, jwt.ErrTokenExpired) || claims.Fid != expectedFid {
		return false
	}
	otherClaims := *claims
	otherClaims.ExpiresAt = nil
	// Do not mistake expired+not-before/issued-at errors for expiry alone. This
	// extra issued-at restriction applies only to classification, never normal
	// authorization. Keep future volume claim policies aligned here.
	return jwt.NewValidator(jwt.WithTimeFunc(func() time.Time { return now }), jwt.WithIssuedAt()).Validate(&otherClaims) == nil
}
