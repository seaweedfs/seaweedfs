//go:build azurekms

package azure

import (
	"bytes"
	"crypto/rand"
	"testing"

	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
)

func TestSplitKeyID(t *testing.T) {
	tests := []struct {
		name        string
		keyID       string
		wantName    string
		wantVersion string
	}{
		{
			name:     "plain key name",
			keyID:    "my-key",
			wantName: "my-key",
		},
		{
			name:     "key url without version",
			keyID:    "https://myvault.vault.azure.net/keys/my-key",
			wantName: "my-key",
		},
		{
			name:        "key url with version",
			keyID:       "https://myvault.vault.azure.net/keys/my-key/abc123",
			wantName:    "my-key",
			wantVersion: "abc123",
		},
		{
			name:     "key url with trailing slash",
			keyID:    "https://myvault.vault.azure.net/keys/my-key/",
			wantName: "my-key",
		},
		{
			name:     "key url with a non keys path",
			keyID:    "https://myvault.vault.azure.net/secrets/my-secret",
			wantName: "https://myvault.vault.azure.net/secrets/my-secret",
		},
		{
			name:     "name that only looks like a url",
			keyID:    "my-key:abc123",
			wantName: "my-key:abc123",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name, version := splitKeyID(test.keyID)
			if name != test.wantName {
				t.Fatalf("name = %q, want %q", name, test.wantName)
			}
			if version != test.wantVersion {
				t.Fatalf("version = %q, want %q", version, test.wantVersion)
			}
		})
	}
}

// The envelope is JSON, so a raw binary wrapped key would be replaced by
// U+FFFD on the way in and could never be decrypted.
func TestCiphertextEnvelopeRoundTrip(t *testing.T) {
	wrapped := make([]byte, 256)
	if _, err := rand.Read(wrapped); err != nil {
		t.Fatalf("generate wrapped key: %v", err)
	}

	keyID := "https://myvault.vault.azure.net/keys/my-key/abc123"
	envelopeBlob, err := seaweedkms.CreateEnvelope("azure", keyID, encodeCiphertext(wrapped), nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}

	envelope, err := seaweedkms.ParseEnvelope(envelopeBlob)
	if err != nil {
		t.Fatalf("parse envelope: %v", err)
	}
	if envelope.KeyID != keyID {
		t.Fatalf("key id = %q, want %q", envelope.KeyID, keyID)
	}

	decoded, err := decodeCiphertext(envelope.Ciphertext)
	if err != nil {
		t.Fatalf("decode ciphertext: %v", err)
	}
	if !bytes.Equal(decoded, wrapped) {
		t.Fatal("decoded wrapped key differs from the encrypted one")
	}
}

func TestDecodeCiphertextRejectsInvalidInput(t *testing.T) {
	for _, ciphertext := range []string{"", "not base64!!", "a"} {
		if _, err := decodeCiphertext(ciphertext); err == nil {
			t.Fatalf("decodeCiphertext(%q) succeeded, want error", ciphertext)
		}
	}
}
