package util

import (
	"strings"
	"testing"
)

type mapConfig map[string]string

func (m mapConfig) GetString(key string) string { return m[key] }

func TestCheckFips140HmacKey(t *testing.T) {
	cases := []struct {
		key      string
		enforced bool
		wantErr  bool
	}{
		{"", true, false},
		{"0123456789", true, true},
		{"0123456789abc", true, true},
		{"0123456789abcd", true, false},
		{"0123456789", false, false},
	}
	for _, tc := range cases {
		setFIPS140Enforced(t, tc.enforced)
		err := CheckFips140HmacKey("jwt.signing.key", []byte(tc.key))
		if (err != nil) != tc.wantErr {
			t.Errorf("%d-byte key, enforced=%v: got %v, want error %v", len(tc.key), tc.enforced, err, tc.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "jwt.signing.key") {
			t.Errorf("error %q does not name the setting", err)
		}
	}
}

func TestCheckFips140SigningKeys(t *testing.T) {
	long := "0123456789abcdef"
	for _, setting := range jwtSigningKeySettings {
		cfg := mapConfig{}
		for _, s := range jwtSigningKeySettings {
			cfg[s] = long
		}
		cfg[setting] = "0123456789"

		setFIPS140Enforced(t, true)
		if err := CheckFips140SigningKeys("security.toml", cfg); err == nil || !strings.Contains(err.Error(), setting) {
			t.Errorf("short %s, enforced: got %v, want an error naming it", setting, err)
		}
		setFIPS140Enforced(t, false)
		if err := CheckFips140SigningKeys("security.toml", cfg); err != nil {
			t.Errorf("short %s, not enforced: got %v", setting, err)
		}
	}
	setFIPS140Enforced(t, true)
	if err := CheckFips140SigningKeys("security.toml", mapConfig{}); err != nil {
		t.Errorf("no keys, enforced: got %v", err)
	}
}

func TestShortJwtKeyWarning(t *testing.T) {
	setFIPS140Enforced(t, false)
	if w := shortJwtKeyWarning("/etc/seaweedfs/security.toml", "jwt.signing.key", []byte("0123456789")); !strings.HasPrefix(w, "/etc/seaweedfs/security.toml: ") || !strings.Contains(w, "jwt.signing.key works, but under GODEBUG=fips140=only this server would not start") || !strings.Contains(w, "(it has 10)") || !strings.Contains(w, JwtSigningKeysWiki) {
		t.Errorf("short key, not enforced: got %q", w)
	}
	if w := shortJwtKeyWarning("security.toml", "jwt.signing.previous_key", []byte("0123456789")); w != "" {
		t.Errorf("short previous key, not enforced: got %q", w)
	}
	for _, key := range []string{"", "0123456789abcd"} {
		if w := shortJwtKeyWarning("security.toml", "jwt.signing.key", []byte(key)); w != "" {
			t.Errorf("%d-byte key, not enforced: got %q, want no warning", len(key), w)
		}
	}
	setFIPS140Enforced(t, true)
	if w := shortJwtKeyWarning("security.toml", "jwt.signing.key", []byte("0123456789")); w != "" {
		t.Errorf("short key, enforced: got %q, want no warning (the key is refused)", w)
	}
}

func TestCheckFips140HmacKeysNamesEveryShortKey(t *testing.T) {
	setFIPS140Enforced(t, true)
	err := CheckFips140HmacKeys(
		[]string{"jwt.signing.key", "jwt.signing.read.key", "jwt.filer_signing.key"},
		[]string{"0123456789", "0123456789abcdef", "01234567"},
	)
	if err == nil {
		t.Fatal("two short keys, enforced: got no error")
	}
	if want := "jwt.signing.key (10 characters) and jwt.filer_signing.key (8 characters) are shorter than the 14 characters FIPS 140 needs for HMAC keys"; err.Error() != want {
		t.Errorf("got %q, want %q", err, want)
	}
	for _, want := range []string{"jwt.signing.key (10 characters)", "jwt.filer_signing.key (8 characters)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "jwt.signing.read.key") {
		t.Errorf("error %q names the 16-byte key", err)
	}
	if one := CheckFips140HmacKeys([]string{"jwt.signing.key"}, []string{"0123456789"}); one == nil || one.Error() != "jwt.signing.key (10 characters) is shorter than the 14 characters FIPS 140 needs for HMAC keys" {
		t.Errorf("one short key: got %v", one)
	}
}
