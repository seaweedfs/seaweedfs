package util

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Fips140MinHmacKeyBytes is the shortest key crypto/hmac accepts under
// GODEBUG=fips140=only: hmac.New panics there on keys under 112 bits.
const Fips140MinHmacKeyBytes = 14

// JwtSigningKeysWiki documents switching JWT signing keys without downtime.
const JwtSigningKeysWiki = "https://github.com/seaweedfs/seaweedfs/wiki/Cryptography-and-FIPS-Compliance#jwt-signing-keys"

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
// shows them all: "jwt.signing.key (10 characters) and jwt.filer_signing.key
// (8 characters) are shorter than the 14 characters FIPS 140 needs for HMAC
// keys".
func CheckFips140HmacKeys(settings []string, keys []string) error {
	var short []string
	for i, setting := range settings {
		if CheckFips140HmacKey(setting, []byte(keys[i])) != nil {
			short = append(short, fmt.Sprintf("%s (%d characters)", setting, len(keys[i])))
		}
	}
	switch len(short) {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("%s is shorter than the %d characters FIPS 140 needs for HMAC keys", short[0], Fips140MinHmacKeyBytes)
	}
	list := strings.Join(short[:len(short)-1], ", ") + " and " + short[len(short)-1]
	return fmt.Errorf("%s are shorter than the %d characters FIPS 140 needs for HMAC keys", list, Fips140MinHmacKeyBytes)
}

// CheckFips140SigningKeys runs CheckFips140HmacKeys on every JWT signing key
// setting in v, read from file. Outside fips140=only, where a short key works,
// it logs shortJwtKeyWarning for each one instead.
func CheckFips140SigningKeys(file string, v interface{ GetString(string) string }) error {
	keys := make([]string, len(jwtSigningKeySettings))
	for i, setting := range jwtSigningKeySettings {
		keys[i] = v.GetString(setting)
		if warning := shortJwtKeyWarning(file, setting, []byte(keys[i])); warning != "" {
			glog.Warning(warning)
		}
	}
	return CheckFips140HmacKeys(jwtSigningKeySettings, keys)
}

// shortJwtKeyWarning returns the warning for a JWT key in file that works
// outside GODEBUG=fips140=only but is under Fips140MinHmacKeyBytes, or "" when
// there is nothing to warn about. The warning asks for a switch to a longer key
// while the short one still works. A previous_key gets none: it holds the old
// key during exactly that switch, and under fips140=only the startup refusal
// names it.
func shortJwtKeyWarning(file, setting string, key []byte) string {
	if len(key) == 0 || len(key) >= Fips140MinHmacKeyBytes || fips140Enforced() || strings.HasSuffix(setting, "previous_key") {
		return ""
	}
	return fmt.Sprintf("%s: this process does not run with GODEBUG=fips140=only, so %s works, but under GODEBUG=fips140=only this server would not start: the key is shorter than the %d characters FIPS 140 needs for HMAC keys (it has %d); to replace it on every server that uses it without downtime, see %s",
		file, setting, Fips140MinHmacKeyBytes, len(key), JwtSigningKeysWiki)
}
