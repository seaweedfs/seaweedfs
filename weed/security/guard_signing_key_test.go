package security

import "testing"

// TestUpdateSigningKeysRotates pins the hot-reload contract: after a key
// rotation the guard validates tokens minted with the new key and rejects the
// old one, so a SIGHUP recovers from a key mismatch without a process restart.
func TestUpdateSigningKeysRotates(t *testing.T) {
	g := NewGuard(nil, "old-write", 10, "old-read", 60)

	g.UpdateSigningKeys("new-write", 11, "new-read", 61)

	if string(g.SigningKey) != "new-write" || g.ExpiresAfterSec != 11 {
		t.Fatalf("write key not refreshed: key=%q exp=%d", g.SigningKey, g.ExpiresAfterSec)
	}
	if string(g.ReadSigningKey) != "new-read" || g.ReadExpiresAfterSec != 61 {
		t.Fatalf("read key not refreshed: key=%q exp=%d", g.ReadSigningKey, g.ReadExpiresAfterSec)
	}

	tok := GenJwtForVolumeServer(g.SigningKey, g.ExpiresAfterSec, "3,01637037d6")
	if _, err := DecodeJwt(g.SigningKey, tok, &SeaweedFileIdClaims{}); err != nil {
		t.Fatalf("token minted with rotated key should verify: %v", err)
	}
	if _, err := DecodeJwt(SigningKey("old-write"), tok, &SeaweedFileIdClaims{}); err == nil {
		t.Fatalf("token minted with rotated key must not verify against the old key")
	}
}

// TestUpdateSigningKeysTogglesWriteActive guards the isWriteActive recompute:
// adding a signing key to an otherwise-open guard must activate auth, and
// clearing it again must deactivate.
func TestUpdateSigningKeysTogglesWriteActive(t *testing.T) {
	g := NewGuard(nil, "", 0, "", 0)
	if g.isWriteActive {
		t.Fatalf("no whitelist and no key should be inactive")
	}

	g.UpdateSigningKeys("write", 10, "", 0)
	if !g.isWriteActive {
		t.Fatalf("setting a signing key should activate auth")
	}

	g.UpdateSigningKeys("", 0, "", 0)
	if g.isWriteActive {
		t.Fatalf("clearing the signing key should deactivate auth")
	}
}
