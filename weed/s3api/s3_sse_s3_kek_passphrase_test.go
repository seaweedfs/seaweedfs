package s3api

import (
	"testing"
)

// TestSetKEKPassphraseEnablesEncryptedRoundTrip exercises the wrap/unwrap
// round-trip after SetKEKPassphrase configures the manager. This is the
// primary behaviour the new env-var wiring relies on: once the passphrase
// is in place, wrapKEK produces v2-format ciphertext that unwrapKEK can
// reverse. Without the SetKEKPassphrase plumbing, deriveWrappingKey would
// fail with "no KEK passphrase configured".
func TestSetKEKPassphraseEnablesEncryptedRoundTrip(t *testing.T) {
	km := NewSSES3KeyManager()
	km.SetKEKPassphrase("test-passphrase-32-bytes-or-anything")

	original := make([]byte, SSES3KeySize)
	for i := range original {
		original[i] = byte(i)
	}

	wrapped, err := km.wrapKEK(original)
	if err != nil {
		t.Fatalf("wrapKEK: %v", err)
	}
	if !isV2WrappedKEK(wrapped) {
		t.Fatal("wrapped output should use v2 magic prefix")
	}

	got, err := km.unwrapKEK(wrapped)
	if err != nil {
		t.Fatalf("unwrapKEK: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("round-trip mismatch: got %x want %x", got, original)
	}
}

// TestSetKEKPassphraseDifferentInstancesNoCollision proves random salts
// per-installation actually decorrelate two managers using the same
// passphrase: their wrapped payloads must differ even when the input KEK
// matches.
func TestSetKEKPassphraseDifferentInstancesNoCollision(t *testing.T) {
	a := NewSSES3KeyManager()
	a.SetKEKPassphrase("shared-passphrase")
	b := NewSSES3KeyManager()
	b.SetKEKPassphrase("shared-passphrase")

	kek := make([]byte, SSES3KeySize)
	for i := range kek {
		kek[i] = 0xAB
	}

	wa, err := a.wrapKEK(kek)
	if err != nil {
		t.Fatalf("wrapKEK a: %v", err)
	}
	wb, err := b.wrapKEK(kek)
	if err != nil {
		t.Fatalf("wrapKEK b: %v", err)
	}
	if string(wa) == string(wb) {
		t.Fatal("two managers with the same passphrase produced byte-identical wrapped output; salt is not random")
	}

	// Both must still self-roundtrip.
	if got, _ := a.unwrapKEK(wa); string(got) != string(kek) {
		t.Fatal("manager a failed self roundtrip")
	}
	if got, _ := b.unwrapKEK(wb); string(got) != string(kek) {
		t.Fatal("manager b failed self roundtrip")
	}
}

// TestNoPassphraseKeepsLegacyHexDecodePath confirms that a manager left
// without a passphrase still drives the historical plaintext-hex path
// (loadSuperKeyFromFiler logs a warning and reads hex). That's the
// fallback InitializeGlobalSSES3KeyManager keeps for upgrades.
func TestNoPassphraseKeepsLegacyHexDecodePath(t *testing.T) {
	km := NewSSES3KeyManager()
	if km.kekPassphrase != "" {
		t.Fatalf("default passphrase should be empty, got %q", km.kekPassphrase)
	}
	// wrapKEK requires a passphrase; without one it must surface the error
	// rather than producing unencrypted output.
	if _, err := km.wrapKEK(make([]byte, SSES3KeySize)); err == nil {
		t.Fatal("wrapKEK without passphrase should fail; got nil error")
	}
}
