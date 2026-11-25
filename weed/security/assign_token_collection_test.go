package security

import (
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

func TestAssignmentTokenBindsCollectionIncludingDefault(t *testing.T) {
	key := SigningKey("assignment-collection-test-key")
	const fid = "3,0100000011"
	for _, collection := range []string{"", "bucket-a"} {
		t.Run(collection, func(t *testing.T) {
			proof := GenJwtForAssign(key, time.Hour, fid, "session", 2, collection)
			if !VerifyAssignJwt(key, nil, proof, fid+"_1", "session", 0, collection) {
				t.Fatal("proof refused in its assigned collection")
			}
			if VerifyAssignJwt(key, nil, proof, fid, "session", 0, "other") {
				t.Fatal("proof accepted in another collection")
			}
		})
	}

	// A pre-collection proof must not silently become a proof for the default
	// collection, even though decoding a missing string would yield empty.
	for _, missing := range []bool{true, false} {
		claims := jwt.MapClaims{"fid": fid, "cnt": 1, "jti": "session", "exp": time.Now().Add(time.Hour).Unix()}
		if !missing {
			claims["collection"] = nil
		}
		raw, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(AssignSigningKey(key)))
		if err != nil {
			t.Fatal(err)
		}
		if VerifyAssignJwt(key, nil, EncodedJwt(raw), fid, "session", 0, "") {
			t.Fatalf("proof without a collection accepted for the default collection (missing=%v)", missing)
		}
	}
}
