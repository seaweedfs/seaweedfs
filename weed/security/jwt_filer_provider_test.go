package security

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
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
			before := time.Now()
			token := provider(tc.isWrite)
			after := time.Now()
			claims := &SeaweedFilerClaims{}
			if _, err := DecodeJwt(SigningKey(tc.signedBy), token, claims); err != nil {
				t.Fatalf("token does not validate against the %s key: %v", tc.name, err)
			}
			if claims.ExpiresAt == nil {
				t.Fatal("token never expires")
			}
			expiresIn := claims.ExpiresAt.Time
			if expiresIn.Before(before.Add(time.Duration(tc.expires-1)*time.Second)) || expiresIn.After(after.Add(time.Duration(tc.expires+1)*time.Second)) {
				t.Fatalf("token expires at %v, want %ds after %v", expiresIn, tc.expires, before)
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

func TestLoadFilerJwtFromFilePartialKeys(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "security.toml")
	config := `
[jwt.filer_signing.read]
key = "side-read-key"
`
	if err := os.WriteFile(configFile, []byte(config), 0644); err != nil {
		t.Fatal(err)
	}

	gv := util.GetViper()
	priorKey := gv.GetString("jwt.filer_signing.key")
	gv.Set("jwt.filer_signing.key", "global-write-key")
	t.Cleanup(func() { gv.Set("jwt.filer_signing.key", priorKey) })

	provider, err := LoadFilerJwtFromFile(configFile)
	if err != nil {
		t.Fatal(err)
	}
	if provider == nil {
		t.Fatal("config with a filer signing key gave a nil provider")
	}

	if _, err := DecodeJwt(SigningKey("global-write-key"), provider(true), &SeaweedFilerClaims{}); err != nil {
		t.Fatalf("write token does not validate against the global write key: %v", err)
	}
	if _, err := DecodeJwt(SigningKey("side-read-key"), provider(false), &SeaweedFilerClaims{}); err != nil {
		t.Fatalf("read token does not validate against the side read key: %v", err)
	}
}

func TestLoadFilerJwtFromFileEnvOverride(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "security.toml")
	config := `
[jwt.filer_signing]
key = "side-write-key"
[jwt.filer_signing.read]
key = "side-read-key"
`
	if err := os.WriteFile(configFile, []byte(config), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WEED_JWT_FILER_SIGNING_READ_KEY", "env-read-key")

	provider, err := LoadFilerJwtFromFile(configFile)
	if err != nil {
		t.Fatal(err)
	}
	if provider == nil {
		t.Fatal("config with filer signing keys gave a nil provider")
	}

	if _, err := DecodeJwt(SigningKey("env-read-key"), provider(false), &SeaweedFilerClaims{}); err != nil {
		t.Fatalf("read token does not validate against the env override key: %v", err)
	}
	if _, err := DecodeJwt(SigningKey("side-write-key"), provider(true), &SeaweedFilerClaims{}); err != nil {
		t.Fatalf("write token does not validate against the side write key: %v", err)
	}
}
