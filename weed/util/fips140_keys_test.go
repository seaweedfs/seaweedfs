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
		if err := CheckFips140SigningKeys(cfg); err == nil || !strings.Contains(err.Error(), setting) {
			t.Errorf("short %s, enforced: got %v, want an error naming it", setting, err)
		}
		setFIPS140Enforced(t, false)
		if err := CheckFips140SigningKeys(cfg); err != nil {
			t.Errorf("short %s, not enforced: got %v", setting, err)
		}
	}
	setFIPS140Enforced(t, true)
	if err := CheckFips140SigningKeys(mapConfig{}); err != nil {
		t.Errorf("no keys, enforced: got %v", err)
	}
}

func TestShortJwtKeyWarning(t *testing.T) {
	setFIPS140Enforced(t, false)
	if w := shortJwtKeyWarning("jwt.signing.key", []byte("0123456789")); !strings.Contains(w, "jwt.signing.key is 10 bytes") || !strings.Contains(w, "future release") || !strings.Contains(w, "previous_key") {
		t.Errorf("short key, not enforced: got %q", w)
	}
	if w := shortJwtKeyWarning("jwt.signing.previous_key", []byte("0123456789")); !strings.Contains(w, "clear it once the key rotation is done") || strings.Contains(w, "future release") {
		t.Errorf("short previous key, not enforced: got %q", w)
	}
	for _, key := range []string{"", "0123456789abcd"} {
		if w := shortJwtKeyWarning("jwt.signing.key", []byte(key)); w != "" {
			t.Errorf("%d-byte key, not enforced: got %q, want no warning", len(key), w)
		}
	}
	setFIPS140Enforced(t, true)
	if w := shortJwtKeyWarning("jwt.signing.key", []byte("0123456789")); w != "" {
		t.Errorf("short key, enforced: got %q, want no warning (the key is refused)", w)
	}
}
