package sse_test

import (
	"crypto/md5"
	"encoding/base64"
	"testing"
)

// TestManualMD5Verification manually verifies the MD5 calculation from the logs
func TestManualMD5Verification(t *testing.T) {
	// From the debug logs:
	// Key: 746573742d6b65792d666f722d64656275 6767696e672d707572706f73657321
	// Key base64: dGVzdC1rZXktZm9yLWRlYnVnZ2luZy1wdXJwb3NlcyE=
	// Key MD5: LQsAwkDX/VBipgcSYJwnpg==
	
	// Decode the base64 key
	keyB64 := "dGVzdC1rZXktZm9yLWRlYnVnZ2luZy1wdXJwb3NlcyE="
	key, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil {
		t.Fatalf("Failed to decode key: %v", err)
	}
	
	t.Logf("Decoded key: %x", key)
	t.Logf("Key string: %s", string(key))
	t.Logf("Key length: %d", len(key))
	
	// Calculate MD5 exactly like SeaweedFS does (s3_sse_c.go:105-106)
	sum := md5.Sum(key)
	expectedMD5 := base64.StdEncoding.EncodeToString(sum[:])
	
	t.Logf("MD5 sum: %x", sum)
	t.Logf("Expected MD5: %s", expectedMD5)
	
	// From logs:
	providedMD5 := "LQsAwkDX/VBipgcSYJwnpg=="
	t.Logf("Provided MD5: %s", providedMD5)
	
	if expectedMD5 == providedMD5 {
		t.Log("✅ MD5 matches! The issue might be elsewhere.")
	} else {
		t.Errorf("❌ MD5 mismatch: expected=%s, provided=%s", expectedMD5, providedMD5)
	}
}
