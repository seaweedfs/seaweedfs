package util

import "fmt"

// Fips140MinHmacKeyBytes is the shortest key crypto/hmac accepts under
// GODEBUG=fips140=only: hmac.New panics there on keys under 112 bits.
const Fips140MinHmacKeyBytes = 14

// jwtSigningKeySettings are the security.toml settings that key HS256 JWTs,
// for signing or for verifying.
var jwtSigningKeySettings = []string{
	"jwt.signing.key",
	"jwt.signing.previous_key",
	"jwt.signing.read.key",
	"jwt.signing.read.previous_key",
	"jwt.filer_signing.key",
	"jwt.filer_signing.previous_key",
	"jwt.filer_signing.read.key",
	"jwt.filer_signing.read.previous_key",
}

// CheckFips140HmacKey returns an error naming setting when GODEBUG=fips140=only
// is in effect and key is set but too short for hmac.New, so a short key is
// refused where it is configured instead of panicking where it is first used.
func CheckFips140HmacKey(setting string, key []byte) error {
	if len(key) == 0 || len(key) >= Fips140MinHmacKeyBytes || !fips140Enforced() {
		return nil
	}
	return fmt.Errorf("%s is %d bytes; GODEBUG=fips140=only requires HMAC keys of at least %d bytes", setting, len(key), Fips140MinHmacKeyBytes)
}

// CheckFips140SigningKeys runs CheckFips140HmacKey on every JWT signing key
// setting in v.
func CheckFips140SigningKeys(v interface{ GetString(string) string }) error {
	for _, setting := range jwtSigningKeySettings {
		if err := CheckFips140HmacKey(setting, []byte(v.GetString(setting))); err != nil {
			return err
		}
	}
	return nil
}
