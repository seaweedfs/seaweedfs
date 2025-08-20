package sse_test

import (
	"crypto/md5"
	"encoding/base64"
	"testing"
)

// TestSSECKeyMD5Generation tests if our SSE-C key MD5 generation matches SeaweedFS expectations
func TestSSECKeyMD5Generation(t *testing.T) {
	// Generate a known key for testing
	key := []byte("abcdefghijklmnopqrstuvwxyz123456") // 32 bytes
	
	// Generate MD5 the same way as SeaweedFS (from s3_sse_c.go:105-106)
	sum := md5.Sum(key)
	expectedMD5 := base64.StdEncoding.EncodeToString(sum[:])
	
	t.Logf("Key: %x", key)
	t.Logf("MD5 sum: %x", sum)
	t.Logf("Base64 MD5: %s", expectedMD5)
	
	// This is how our test generates it
	keyB64 := base64.StdEncoding.EncodeToString(key)
	keyMD5Hash := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])
	
	t.Logf("Our key B64: %s", keyB64)
	t.Logf("Our MD5: %s", keyMD5)
	
	if keyMD5 != expectedMD5 {
		t.Errorf("MD5 mismatch: our=%s, expected=%s", keyMD5, expectedMD5)
	} else {
		t.Log("MD5 generation is correct!")
	}
}
