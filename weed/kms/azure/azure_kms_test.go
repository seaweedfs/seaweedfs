//go:build azurekms

package azure

import (
	"bytes"
	"crypto/rand"
	"testing"

	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
)

func TestSplitKeyID(t *testing.T) {
	provider := &AzureKMSProvider{vaultURL: "https://myvault.vault.azure.net"}

	tests := []struct {
		name        string
		keyID       string
		wantName    string
		wantVersion string
		wantErr     bool
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
			name:     "key url with a mixed case host",
			keyID:    "https://MyVault.vault.azure.net/keys/my-key",
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
		{
			name:    "key url from another vault",
			keyID:   "https://othervault.vault.azure.net/keys/my-key",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name, version, err := provider.splitKeyID(test.keyID)
			if test.wantErr {
				if err == nil {
					t.Fatalf("splitKeyID(%q) succeeded, want error", test.keyID)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitKeyID(%q): %v", test.keyID, err)
			}
			if name != test.wantName {
				t.Fatalf("name = %q, want %q", name, test.wantName)
			}
			if version != test.wantVersion {
				t.Fatalf("version = %q, want %q", version, test.wantVersion)
			}
		})
	}
}

// The client addresses only the vault it was built for, so a key URL naming a
// different vault must not resolve to this vault's same-named key.
func TestSplitKeyIDRejectsForeignVault(t *testing.T) {
	provider := &AzureKMSProvider{vaultURL: "https://myvault.vault.azure.net/"}

	if _, _, err := provider.splitKeyID("https://evil.vault.azure.net/keys/my-key/abc123"); err == nil {
		t.Fatal("splitKeyID accepted a key URL from another vault")
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

// RSA-OAEP cannot authenticate the encryption context, so the context is
// bound to the wrapped key via a digest in the envelope.
func TestEncryptionContextBinding(t *testing.T) {
	context := map[string]string{"aws:s3:bucket": "bucket", "aws:s3:object": "key"}
	envelopeBlob, err := seaweedkms.CreateEnvelope("azure", "my-key", "d3JhcHBlZA==", map[string]interface{}{
		"encryption_context_sha256": contextDigest(context),
	})
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	envelope, err := seaweedkms.ParseEnvelope(envelopeBlob)
	if err != nil {
		t.Fatalf("parse envelope: %v", err)
	}

	if err := checkContext(envelope, context); err != nil {
		t.Fatalf("matching context rejected: %v", err)
	}
	if err := checkContext(envelope, map[string]string{"aws:s3:object": "other"}); err == nil {
		t.Fatal("mismatched context accepted")
	}
	if err := checkContext(envelope, nil); err == nil {
		t.Fatal("missing context accepted for a context-bound key")
	}
}
