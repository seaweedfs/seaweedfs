package s3api

import "testing"

func TestVerifyKeyCommitment_DefaultAcceptsMissing(t *testing.T) {
	// Default behaviour mirrors AWS: a missing commitment field is treated
	// as a legacy object and accepted. This is the cushion that lets
	// operators upgrade without breaking pre-commitment uploads.
	prev := requireKeyCommitment.Load()
	t.Cleanup(func() { requireKeyCommitment.Store(prev) })
	requireKeyCommitment.Store(false)

	if err := VerifyKeyCommitment([]byte("k"), []byte("iv"), "AES256", nil); err != nil {
		t.Fatalf("default path should accept missing commitment, got: %v", err)
	}
}

func TestVerifyKeyCommitment_StrictRejectsMissing(t *testing.T) {
	// Strict mode: any object whose metadata lacks the commitment field is
	// rejected. Closes the silent-downgrade vector — an attacker who can
	// strip the commitment from metadata can no longer bypass verification.
	prev := requireKeyCommitment.Load()
	t.Cleanup(func() { requireKeyCommitment.Store(prev) })
	requireKeyCommitment.Store(true)

	err := VerifyKeyCommitment([]byte("k"), []byte("iv"), "AES256", nil)
	if err == nil {
		t.Fatal("strict mode should reject missing commitment; got nil error")
	}
}

func TestVerifyKeyCommitment_StrictAcceptsValidCommitment(t *testing.T) {
	// Strict mode does not change the verification outcome for objects that
	// do carry a commitment — the only behavioural delta is the missing
	// case.
	prev := requireKeyCommitment.Load()
	t.Cleanup(func() { requireKeyCommitment.Store(prev) })
	requireKeyCommitment.Store(true)

	key := []byte("strict-mode-test-key")
	// IV here is just an opaque input to the HMAC commitment; the test
	// doesn't pass it into AES-CTR so it doesn't have to be the AES block
	// size. The 16 bytes match the AES block size to keep the literal
	// realistic.
	iv := []byte("strict-mode-iv16")
	commit := ComputeKeyCommitment(key, iv, "AES256")
	if err := VerifyKeyCommitment(key, iv, "AES256", commit); err != nil {
		t.Fatalf("strict mode should accept valid commitment, got: %v", err)
	}
}

func TestSetRequireKeyCommitment(t *testing.T) {
	prev := requireKeyCommitment.Load()
	t.Cleanup(func() { requireKeyCommitment.Store(prev) })

	SetRequireKeyCommitment(true)
	if !requireKeyCommitment.Load() {
		t.Fatal("SetRequireKeyCommitment(true) did not propagate to the atomic")
	}
	SetRequireKeyCommitment(false)
	if requireKeyCommitment.Load() {
		t.Fatal("SetRequireKeyCommitment(false) did not propagate to the atomic")
	}
}
