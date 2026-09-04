package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// An operator-configured key always wins and must not trigger generation.
func TestResolveFilerAdminSigningKey_UsesConfiguredKey(t *testing.T) {
	t.Setenv("WEED_JWT_FILER_SIGNING_KEY", "operator-configured-key")
	dir := t.TempDir()

	got := resolveFilerAdminSigningKey(dir)
	if string(got) != "operator-configured-key" {
		t.Fatalf("resolveFilerAdminSigningKey = %q, want the configured key", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ".filer_signing_key")); !os.IsNotExist(err) {
		t.Fatalf("expected no generated key file when a key is configured, stat err = %v", err)
	}
}

// With no configured key, a fresh install must never fall back to an empty
// (unauthenticated) key: it generates one, persists it, and reuses the same
// value across restarts.
func TestResolveFilerAdminSigningKey_GeneratesAndPersists(t *testing.T) {
	t.Setenv("WEED_JWT_FILER_SIGNING_KEY", "")
	dir := t.TempDir()

	first := resolveFilerAdminSigningKey(dir)
	if len(first) == 0 {
		t.Fatal("resolveFilerAdminSigningKey returned an empty key with none configured")
	}
	if string(first) != util.GetViper().GetString("jwt.filer_signing.key") {
		t.Fatal("generated key was not exported as WEED_JWT_FILER_SIGNING_KEY for same-process callers")
	}

	// Simulate a restart: forget the in-process env var, keep the persisted file.
	os.Unsetenv("WEED_JWT_FILER_SIGNING_KEY")
	second := resolveFilerAdminSigningKey(dir)
	if string(second) != string(first) {
		t.Fatalf("key changed across restarts: first=%q second=%q", first, second)
	}
}

// A directory that can't be written to must degrade to an empty key rather
// than panicking or crashing the filer.
func TestResolveFilerAdminSigningKey_UnwritableDirReturnsEmpty(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	t.Setenv("WEED_JWT_FILER_SIGNING_KEY", "")
	root := t.TempDir()
	unwritable := filepath.Join(root, "ro")
	if err := os.MkdirAll(unwritable, 0555); err != nil {
		t.Fatalf("mkdir %s: %v", unwritable, err)
	}

	got := resolveFilerAdminSigningKey(filepath.Join(unwritable, "child"))
	if len(got) != 0 {
		t.Fatalf("resolveFilerAdminSigningKey = %q, want empty when persistence is impossible", got)
	}
}
