package security

import (
	"errors"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

func TestGenJwtForFilerScopeCarriesReadWriteAndReadOnlyClaims(t *testing.T) {
	token := GenJwtForFilerScope(SigningKey("scope-test-signing-key"), time.Hour, "lease-1", []string{"/write"}, []string{"/read"})
	if token == "" {
		t.Fatal("GenJwtForFilerScope returned an empty token")
	}
	claims := &SeaweedFilerClaims{}
	parsed, err := DecodeJwt(SigningKey("scope-test-signing-key"), token, claims)
	if err != nil || parsed == nil || !parsed.Valid {
		t.Fatalf("decode scoped token: valid=%v err=%v", parsed != nil && parsed.Valid, err)
	}
	if claims.ID != "lease-1" || !claims.IsScoped() {
		t.Fatalf("claims = %+v, want scoped lease-1 claims", claims)
	}
	if len(claims.AllowedPrefixes) != 1 || claims.AllowedPrefixes[0] != "/write" {
		t.Fatalf("allowed prefixes = %v", claims.AllowedPrefixes)
	}
	if len(claims.ReadPrefixes) != 1 || claims.ReadPrefixes[0] != "/read" {
		t.Fatalf("read prefixes = %v", claims.ReadPrefixes)
	}
}

func TestGenJwtForFilerScopeRequiresKeyAndPositiveExpiry(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  SigningKey
		age  time.Duration
	}{
		{name: "missing key", age: time.Hour},
		{name: "zero expiry", key: SigningKey("scope-test-signing-key")},
		{name: "negative expiry", key: SigningKey("scope-test-signing-key"), age: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := GenJwtForFilerScope(tc.key, tc.age, "lease-1", []string{"/write"}, nil); got != "" {
				t.Fatal("expected no token")
			}
		})
	}
}

func TestGenJwtForFilerScopeRequiresAPathGrant(t *testing.T) {
	for _, tc := range []struct {
		name     string
		allowed  []string
		readOnly []string
	}{
		{name: "nil grants"},
		{name: "empty grants", allowed: []string{}, readOnly: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := GenJwtForFilerScope(SigningKey("scope-test-signing-key"), time.Hour, "lease-1", tc.allowed, tc.readOnly); got != "" {
				t.Fatal("expected no token without a path grant")
			}
		})
	}
}

func TestDecodeJwtWithAcceptsCurrentAndPreviousScopeKeys(t *testing.T) {
	current := SigningKey("current-scope-key")
	previous := SigningKey("previous-scope-key")
	other := SigningKey("other-scope-key")
	for _, tc := range []struct {
		name        string
		signedBy    SigningKey
		previousKey SigningKey
		wantErr     bool
	}{
		{name: "current key", signedBy: current},
		{name: "previous key", signedBy: previous, previousKey: previous},
		{name: "wrong previous key", signedBy: previous, previousKey: other, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := GenJwtForFilerScope(tc.signedBy, time.Hour, "lease-1", []string{"/write"}, nil)
			claims := &SeaweedFilerClaims{}
			parsed, err := DecodeJwtWith(current, tc.previousKey, token, claims, 0)
			if tc.wantErr {
				if err == nil || parsed != nil && parsed.Valid {
					t.Fatalf("wrong previous key accepted token: parsed=%v err=%v", parsed, err)
				}
				return
			}
			if err != nil || parsed == nil || !parsed.Valid {
				t.Fatalf("scope key rotation decode: parsed=%v err=%v", parsed, err)
			}
		})
	}

	expiredPayload, err := jwt.NewWithClaims(jwt.SigningMethodHS256, SeaweedFilerClaims{
		AllowedPrefixes: []string{"/write"},
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        "lease-1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		},
	}).SignedString([]byte(previous))
	if err != nil {
		t.Fatalf("sign expired token: %v", err)
	}
	_, err = DecodeJwtWith(current, previous, EncodedJwt(expiredPayload), &SeaweedFilerClaims{}, 0)
	if !errors.Is(err, jwt.ErrTokenExpired) {
		t.Fatalf("expired previous-key token error = %v, want ErrTokenExpired", err)
	}
}
