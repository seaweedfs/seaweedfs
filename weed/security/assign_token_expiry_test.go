package security

import (
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

func TestAssignmentTokenWithNonPositiveLifetimeUsesDefaultExpiry(t *testing.T) {
	key := SigningKey("assignment-expiry-key-0123456789")
	for _, lifetime := range []time.Duration{0, -time.Minute} {
		t.Run(lifetime.String(), func(t *testing.T) {
			before := time.Now().Add(DefaultAssignTokenLifetime - 2*time.Second)
			token := GenJwtForAssign(key, lifetime, "3,0100000011", "session-1", 1, "")
			claims := &SeaweedAssignClaims{}
			parsed, err := DecodeJwtWith(AssignSigningKey(key), nil, token, claims, 0)
			if err != nil || parsed == nil || !parsed.Valid {
				t.Fatalf("decode default-lifetime token: parsed=%v err=%v", parsed, err)
			}
			if claims.ExpiresAt == nil {
				t.Fatal("default-lifetime assignment token has no expiry")
			}
			if claims.ExpiresAt.Time.Before(before) {
				t.Fatalf("expiry %v is earlier than default lifetime window starting %v", claims.ExpiresAt.Time, before)
			}
			if claims.ExpiresAt.Time.After(time.Now().Add(DefaultAssignTokenLifetime)) {
				t.Fatal("expiry exceeds the default lifetime")
			}
			if !VerifyAssignJwt(key, nil, token, claims.Fid, claims.ID, 0, "") {
				t.Fatal("fresh default-lifetime assignment token did not verify")
			}
		})
	}
}

func TestVerifyAssignJwtRejectsNonExpiringAndExpiredProofs(t *testing.T) {
	key := SigningKey("assignment-expiry-check-key-0123456789")
	fid := "3,0100000011"
	holder := "session-1"

	noExpiry := signAssignmentClaims(t, key, SeaweedAssignClaims{
		Fid:              fid,
		Count:            1,
		RegisteredClaims: jwtRegisteredClaims(holder, time.Time{}),
	})
	if VerifyAssignJwt(key, nil, noExpiry, fid, holder, 0, "") {
		t.Fatal("non-expiring assignment proof was accepted")
	}

	expired := signAssignmentClaims(t, key, SeaweedAssignClaims{
		Fid:              fid,
		Count:            1,
		RegisteredClaims: jwtRegisteredClaims(holder, time.Now().Add(-time.Hour)),
	})
	if VerifyAssignJwt(key, nil, expired, fid, holder, 0, "") {
		t.Fatal("expired assignment proof was accepted")
	}
}

func jwtRegisteredClaims(holder string, expiry time.Time) jwt.RegisteredClaims {
	claims := jwt.RegisteredClaims{ID: holder, IssuedAt: jwt.NewNumericDate(time.Now())}
	if !expiry.IsZero() {
		claims.ExpiresAt = jwt.NewNumericDate(expiry)
	}
	return claims
}

func signAssignmentClaims(t *testing.T, key SigningKey, claims SeaweedAssignClaims) EncodedJwt {
	t.Helper()
	// Existing negative expiry/count tests must fail for their intended reason,
	// not because the collection claim is absent.
	if claims.Collection == nil {
		collection := ""
		claims.Collection = &collection
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(AssignSigningKey(key)))
	if err != nil {
		t.Fatalf("sign assignment claims: %v", err)
	}
	return EncodedJwt(token)
}
