package security

import (
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestVolumeJwtExpiryOnly(t *testing.T) {
	now := time.Unix(1900000000, 0)
	key := SigningKey("disposable-volume-classifier")
	for _, tc := range []struct {
		name                                            string
		expired, nbf, iat, wrongKey, wrongFid, noExpiry bool
		want                                            bool
	}{
		{name: "expired_only", expired: true, want: true},
		{name: "expired_future_nbf", expired: true, nbf: true},
		{name: "expired_future_iat", expired: true, iat: true},
		{name: "expired_bad_signature", expired: true, wrongKey: true},
		{name: "expired_wrong_fid", expired: true, wrongFid: true},
		{name: "valid"}, {name: "future_nbf", nbf: true}, {name: "no_expiry", noExpiry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fid := "1,0000000100001234"
			if tc.wrongFid {
				fid = "1,0000000200001234"
			}
			exp := now.Add(time.Minute)
			if tc.expired {
				exp = now.Add(-time.Second)
			}
			claims := SeaweedFileIdClaims{Fid: fid, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(exp)}}
			if tc.noExpiry {
				claims.ExpiresAt = nil
			}
			if tc.nbf {
				claims.NotBefore = jwt.NewNumericDate(now.Add(time.Minute))
			}
			if tc.iat {
				claims.IssuedAt = jwt.NewNumericDate(now.Add(time.Minute))
			}
			signingKey := key
			if tc.wrongKey {
				signingKey = SigningKey("different-disposable-key")
			}
			token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signingKey))
			require.NoError(t, err)
			require.Equal(t, tc.want, isExpiredVolumeJwtAt(key, EncodedJwt(token), "1,0000000100001234", now))
		})
	}
	require.False(t, isExpiredVolumeJwtAt(key, "malformed", "1,0000000100001234", now))
	require.False(t, isExpiredVolumeJwtAt(nil, "malformed", "1,0000000100001234", now))
	// Bad temporal field types must fail parsing instead of producing a marker.
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"fid": "1,0000000100001234", "exp": now.Add(-time.Minute).Unix(), "nbf": "not-a-date"}).SignedString([]byte(key))
	require.NoError(t, err)
	require.False(t, isExpiredVolumeJwtAt(key, EncodedJwt(token), "1,0000000100001234", now))
}

func TestVolumeJwtExpiryAlgorithmBoundary(t *testing.T) {
	now := time.Unix(1900000000, 0)
	key := SigningKey("disposable-algorithm-test")
	claims := SeaweedFileIdClaims{Fid: "1,0000000100001234", RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(now.Add(-time.Second))}}
	// Existing DecodeJwt permits the HMAC family, not just HS256.
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS512, claims).SignedString([]byte(key))
	require.NoError(t, err)
	require.True(t, isExpiredVolumeJwtAt(key, EncodedJwt(token), claims.Fid, now))
	unsigned, err := jwt.NewWithClaims(jwt.SigningMethodNone, claims).SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	require.False(t, isExpiredVolumeJwtAt(key, EncodedJwt(unsigned), claims.Fid, now))
}
