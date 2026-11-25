package security

import (
	"crypto/fips140"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runUnderFIPS140Only re-runs the calling test in a child process with
// GODEBUG=fips140=only and reports whether the caller is that child. The mode
// is fixed at program start, so the parent only checks that the child passed.
func runUnderFIPS140Only(t *testing.T) bool {
	t.Helper()
	if fips140.Enforced() {
		return true
	}
	godebug := "fips140=only"
	if v := os.Getenv("GODEBUG"); v != "" {
		godebug = v + "," + godebug
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "GODEBUG="+godebug)
	out, err := cmd.CombinedOutput()
	if !strings.Contains(string(out), "=== RUN   "+t.Name()) {
		t.Skipf("test binary does not start with GODEBUG=fips140=only: %v\n%s", err, out)
	}
	if err != nil || !strings.Contains(string(out), "--- PASS: "+t.Name()) {
		t.Fatalf("with GODEBUG=fips140=only: %v\n%s", err, out)
	}
	return false
}

// TestUpdateSigningKeysUnderFIPS140Only checks that a reload under
// fips140=only applies new current keys while dropping short previous keys, so
// servers that pass previous keys and servers that do not end up with the same
// current keys, and that a short current key keeps the keys in effect.
func TestUpdateSigningKeysUnderFIPS140Only(t *testing.T) {
	if !runUnderFIPS140Only(t) {
		return
	}
	oldKey, newKey, short := "0123456789abcdefghij", "abcdefghij0123456789", "0123456789"

	g := NewGuard(nil, oldKey, 10, oldKey, 10, "", "")
	if !g.UpdateSigningKeys(newKey, 10, newKey, 10, short, short) {
		t.Fatal("reload with long current keys and short previous keys reported failure")
	}
	st := g.state.Load()
	if string(st.signingKey) != newKey || string(st.readSigningKey) != newKey {
		t.Fatalf("current keys after reload with short previous keys: %q, %q; want the new key", st.signingKey, st.readSigningKey)
	}
	if len(st.previousSigningKey) != 0 || len(st.previousReadSigningKey) != 0 {
		t.Fatalf("short previous keys kept: %q, %q", st.previousSigningKey, st.previousReadSigningKey)
	}

	if g.UpdateSigningKeys(short, 10, newKey, 10, "", "") {
		t.Fatal("reload with a short current key reported success")
	}
	if st := g.state.Load(); string(st.signingKey) != newKey {
		t.Fatalf("a short current key replaced the key in effect: %q", st.signingKey)
	}
}

// TestLoadFilerJwtFromFileUnderFIPS140Only checks that a filer.sync security
// file is judged only by the filer keys it is used for.
func TestLoadFilerJwtFromFileUnderFIPS140Only(t *testing.T) {
	if !runUnderFIPS140Only(t) {
		return
	}
	write := func(body string) string {
		p := filepath.Join(t.TempDir(), "security.toml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	unused := write("[jwt.filer_signing]\nkey = \"0123456789abcdefghij\"\n[jwt.filer_signing.read]\nkey = \"abcdefghij0123456789\"\n[jwt.signing]\nprevious_key = \"0123456789\"\n")
	if p, err := LoadFilerJwtFromFile(unused); err != nil || p == nil {
		t.Fatalf("long filer keys and a short unused key: got %v, %v; want a provider", p, err)
	}

	short := write("[jwt.filer_signing]\nkey = \"0123456789\"\n[jwt.filer_signing.read]\nkey = \"abcdefghij0123456789\"\n")
	if _, err := LoadFilerJwtFromFile(short); err == nil || !strings.Contains(err.Error(), "jwt.filer_signing.key (10 characters)") {
		t.Fatalf("short filer key: got %v; want an error naming it", err)
	}
}
