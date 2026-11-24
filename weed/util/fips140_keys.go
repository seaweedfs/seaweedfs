package util

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

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
	"jwt.filer_signing.subscribe.key",
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

// CheckFips140HmacKeys is CheckFips140HmacKey for several keys, settings[i]
// naming keys[i]. Its error names every key that is too short, so one startup
// shows them all.
func CheckFips140HmacKeys(settings []string, keys []string) error {
	var short []string
	for i, setting := range settings {
		if CheckFips140HmacKey(setting, []byte(keys[i])) != nil {
			short = append(short, fmt.Sprintf("%s is %d bytes", setting, len(keys[i])))
		}
	}
	if len(short) == 0 {
		return nil
	}
	return fmt.Errorf("%s; GODEBUG=fips140=only requires HMAC keys of at least %d bytes", strings.Join(short, ", "), Fips140MinHmacKeyBytes)
}

// CheckFips140SigningKeys runs CheckFips140HmacKeys on every JWT signing key
// setting in v. Outside fips140=only, where a short key works, it logs
// shortJwtKeyWarning for each one instead.
func CheckFips140SigningKeys(v interface{ GetString(string) string }) error {
	keys := make([]string, len(jwtSigningKeySettings))
	for i, setting := range jwtSigningKeySettings {
		keys[i] = v.GetString(setting)
		if warning := shortJwtKeyWarning(setting, []byte(keys[i])); warning != "" {
			glog.Warning(warning)
		}
	}
	return CheckFips140HmacKeys(jwtSigningKeySettings, keys)
}

// shortJwtKeyWarning returns the warning for a JWT key that works outside
// GODEBUG=fips140=only but is under Fips140MinHmacKeyBytes, or "" when there is
// nothing to warn about. A future release may refuse such keys in every mode,
// so the warning asks for a rotation to a longer key with previous_key while it
// still works. A previous_key holds the old key during that rotation and is
// only refused under fips140=only, so its warning asks to clear it afterwards.
func shortJwtKeyWarning(setting string, key []byte) string {
	if len(key) == 0 || len(key) >= Fips140MinHmacKeyBytes || fips140Enforced() {
		return ""
	}
	if strings.HasSuffix(setting, "previous_key") {
		return fmt.Sprintf("%s is %d bytes; GODEBUG=fips140=only refuses HMAC keys under %d bytes, so clear it once the key rotation is done",
			setting, len(key), Fips140MinHmacKeyBytes)
	}
	return fmt.Sprintf("%s is %d bytes; GODEBUG=fips140=only refuses HMAC keys under %d bytes, and a future release may refuse them in every mode; rotate it to a key of at least %d bytes with previous_key, see security.toml",
		setting, len(key), Fips140MinHmacKeyBytes, Fips140MinHmacKeyBytes)
}
