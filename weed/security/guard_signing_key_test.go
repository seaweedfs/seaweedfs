package security

import (
	"sync"
	"testing"
)

// TestUpdateSigningKeysRotates pins the hot-reload contract: after a key
// rotation the guard validates tokens minted with the new key and rejects the
// old one, so a SIGHUP recovers from a key mismatch without a process restart.
func TestUpdateSigningKeysRotates(t *testing.T) {
	g := NewGuard(nil, "old-write", 10, "old-read", 60)

	g.UpdateSigningKeys("new-write", 11, "new-read", 61)

	if string(g.SigningKey()) != "new-write" || g.ExpiresAfterSec() != 11 {
		t.Fatalf("write key not refreshed: key=%q exp=%d", g.SigningKey(), g.ExpiresAfterSec())
	}
	if string(g.ReadSigningKey()) != "new-read" || g.ReadExpiresAfterSec() != 61 {
		t.Fatalf("read key not refreshed: key=%q exp=%d", g.ReadSigningKey(), g.ReadExpiresAfterSec())
	}

	tok := GenJwtForVolumeServer(g.SigningKey(), g.ExpiresAfterSec(), "3,01637037d6")
	if _, err := DecodeJwt(g.SigningKey(), tok, &SeaweedFileIdClaims{}); err != nil {
		t.Fatalf("token minted with rotated key should verify: %v", err)
	}
	if _, err := DecodeJwt(SigningKey("old-write"), tok, &SeaweedFileIdClaims{}); err == nil {
		t.Fatalf("token minted with rotated key must not verify against the old key")
	}
}

// TestUpdateSigningKeysConcurrent rotates keys and whitelist while readers mint
// and verify tokens and check whitelist membership, so `go test -race` would
// flag any torn read of the signing-key slice header or the whitelist maps.
// Each key rotation uses matching keys so every reader's token validates.
func TestUpdateSigningKeysConcurrent(t *testing.T) {
	g := NewGuard([]string{"10.0.0.1"}, "k0", 60, "k0", 60)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := g.SigningKey()
				tok := GenJwtForVolumeServer(key, g.ExpiresAfterSec(), "3,01637037d6")
				if _, err := DecodeJwt(key, tok, &SeaweedFileIdClaims{}); err != nil {
					t.Errorf("token should verify against the snapshot it was minted from: %v", err)
					return
				}
				g.IsWhiteListed("10.0.0.1")
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		g.UpdateSigningKeys("kw", 60, "kr", 60)
		g.UpdateSigningKeys("kw2", 60, "kr2", 60)
		g.UpdateWhiteList([]string{"10.0.0.1", "192.168.0.0/16"})
	}
	wg.Wait()
}

// TestUpdateSigningKeysTogglesWriteActive guards the isWriteActive recompute:
// adding a signing key to an otherwise-open guard must activate auth, and
// clearing it again must deactivate.
func TestUpdateSigningKeysTogglesWriteActive(t *testing.T) {
	g := NewGuard(nil, "", 0, "", 0)
	if g.state.Load().isWriteActive {
		t.Fatalf("no whitelist and no key should be inactive")
	}

	g.UpdateSigningKeys("write", 10, "", 0)
	if !g.state.Load().isWriteActive {
		t.Fatalf("setting a signing key should activate auth")
	}

	g.UpdateSigningKeys("", 0, "", 0)
	if g.state.Load().isWriteActive {
		t.Fatalf("clearing the signing key should deactivate auth")
	}
}
