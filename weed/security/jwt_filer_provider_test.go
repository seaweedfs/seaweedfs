package security

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFilerJwtFromFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "security.toml")
	config := `
[jwt]
[jwt.filer_signing]
key = "side-write-key"
expires_after_seconds = 30
[jwt.filer_signing.read]
key = "side-read-key"
expires_after_seconds = 90
`
	if err := os.WriteFile(configFile, []byte(config), 0644); err != nil {
		t.Fatal(err)
	}

	provider, err := LoadFilerJwtFromFile(configFile)
	if err != nil {
		t.Fatal(err)
	}
	if provider == nil {
		t.Fatal("config with filer signing keys gave a nil provider")
	}

	for _, tc := range []struct {
		name     string
		isWrite  bool
		signedBy string
		otherKey string
		expires  int64
	}{
		{"read", false, "side-read-key", "side-write-key", 90},
		{"write", true, "side-write-key", "side-read-key", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := provider(tc.isWrite)
			claims := &SeaweedFilerClaims{}
			if _, err := DecodeJwt(SigningKey(tc.signedBy), token, claims); err != nil {
				t.Fatalf("token does not validate against the %s key: %v", tc.name, err)
			}
			if claims.ExpiresAt == nil {
				t.Fatal("token never expires")
			}
			if _, err := DecodeJwt(SigningKey(tc.otherKey), token, &SeaweedFilerClaims{}); err == nil {
				t.Fatal("token also validates against the other access level's key")
			}
		})
	}
}

func TestLoadFilerJwtFromFileWithoutKeys(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "security.toml")
	if err := os.WriteFile(configFile, []byte("[grpc.client]\n"), 0644); err != nil {
		t.Fatal(err)
	}

	provider, err := LoadFilerJwtFromFile(configFile)
	if err != nil {
		t.Fatal(err)
	}
	if provider != nil {
		t.Fatal("config without filer signing keys gave a provider")
	}
}

func TestLoadFilerJwtFromFileMissing(t *testing.T) {
	if _, err := LoadFilerJwtFromFile(filepath.Join(t.TempDir(), "none.toml")); err == nil {
		t.Fatal("missing config file loaded without error")
	}
}
