//go:build azurekms

package azure

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys"

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
			name:    "key url with a non keys path",
			keyID:   "https://myvault.vault.azure.net/secrets/my-secret",
			wantErr: true,
		},
		{
			name:    "key url with extra path segments",
			keyID:   "https://myvault.vault.azure.net/keys/my-key/abc123/extra",
			wantErr: true,
		},
		{
			name:    "key url with an empty key name",
			keyID:   "https://myvault.vault.azure.net/keys//abc123",
			wantErr: true,
		},
		{
			name:     "name that only looks like a url",
			keyID:    "my-key:abc123",
			wantName: "my-key:abc123",
		},
		{
			name:     "key url with explicit default port",
			keyID:    "https://myvault.vault.azure.net:443/keys/my-key",
			wantName: "my-key",
		},
		{
			name:    "key url with non-default port",
			keyID:   "https://myvault.vault.azure.net:8443/keys/my-key",
			wantErr: true,
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
func TestSplitKeyIDExplicitDefaultPort(t *testing.T) {
	provider := &AzureKMSProvider{vaultURL: "https://myvault.vault.azure.net:443"}
	name, _, err := provider.splitKeyID("https://myvault.vault.azure.net/keys/my-key")
	if err != nil || name != "my-key" {
		t.Fatalf("splitKeyID = %q, %v; want my-key, nil", name, err)
	}
}

func TestSplitKeyIDRejectsForeignVault(t *testing.T) {
	provider := &AzureKMSProvider{vaultURL: "https://myvault.vault.azure.net/"}

	if _, _, err := provider.splitKeyID("https://evil.vault.azure.net/keys/my-key/abc123"); err == nil {
		t.Fatal("splitKeyID accepted a key URL from another vault")
	}
}

// A trailing DNS dot names the same vault, so both sides of the comparison are
// normalized the same way.
func TestSplitKeyIDTrailingDot(t *testing.T) {
	tests := []struct {
		name     string
		vaultURL string
		keyID    string
	}{
		{
			name:     "trailing dot on the key url",
			vaultURL: "https://myvault.vault.azure.net",
			keyID:    "https://myvault.vault.azure.net./keys/my-key/abc123",
		},
		{
			name:     "trailing dot on the configured vault",
			vaultURL: "https://myvault.vault.azure.net./",
			keyID:    "https://myvault.vault.azure.net/keys/my-key/abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &AzureKMSProvider{vaultURL: tt.vaultURL}
			name, version, err := provider.splitKeyID(tt.keyID)
			if err != nil || name != "my-key" || version != "abc123" {
				t.Fatalf("splitKeyID = %q, %q, %v; want my-key, abc123, nil", name, version, err)
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

type fakeCredential struct{}

func (fakeCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type fakeTransport struct {
	do func(*http.Request) (*http.Response, error)
}

func (t fakeTransport) Do(req *http.Request) (*http.Response, error) {
	return t.do(req)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// Drive GenerateDataKey and Decrypt through a fake Key Vault transport so the
// whole path — key ID split, request body shape, envelope encoding — is
// exercised, not just the helpers.
func TestGenerateDataKeyDecryptRoundTrip(t *testing.T) {
	vaultURL := "https://myvault.vault.azure.net"
	var wrapped []byte
	var sawAAD bool

	client, err := azkeys.NewClient(vaultURL, fakeCredential{}, &azkeys.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: fakeTransport{do: func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("Authorization") == "" {
					// Key Vault answers an unauthenticated request with the
					// challenge the client then satisfies.
					resp := jsonResponse(401, `{"error":{"code":"Unauthorized"}}`)
					resp.Header.Set("WWW-Authenticate", `Bearer authorization="https://login.windows.net/tenant", resource="https://vault.azure.net"`)
					return resp, nil
				}
				var body []byte
				if rc, err := req.GetBody(); err == nil {
					body, _ = io.ReadAll(rc)
					rc.Close()
				} else if req.Body != nil {
					body, _ = io.ReadAll(req.Body)
				}
				sawAAD = bytes.Contains(body, []byte(`"aad"`))
				var params struct {
					Value string `json:"value"`
				}
				if err := json.Unmarshal(body, &params); err != nil {
					return nil, err
				}
				value, err := base64.RawURLEncoding.DecodeString(params.Value)
				if err != nil {
					return nil, err
				}
				switch {
				case strings.HasSuffix(req.URL.Path, "/encrypt"):
					wrapped = value
					return jsonResponse(200, `{"kid":"`+vaultURL+`/keys/my-key/abc123","value":"`+base64.RawURLEncoding.EncodeToString(wrapped)+`"}`), nil
				case strings.HasSuffix(req.URL.Path, "/decrypt"):
					if !bytes.Equal(value, wrapped) {
						return jsonResponse(400, `{"error":{"code":"BadParameter"}}`), nil
					}
					return jsonResponse(200, `{"kid":"`+vaultURL+`/keys/my-key/abc123","value":"`+params.Value+`"}`), nil
				default:
					return jsonResponse(404, `{"error":{"code":"NotFound"}}`), nil
				}
			}},
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	provider := &AzureKMSProvider{client: client, vaultURL: vaultURL}
	contextMap := map[string]string{"aws:s3:bucket": "bucket"}

	resp, err := provider.GenerateDataKey(context.Background(), &seaweedkms.GenerateDataKeyRequest{
		KeyID:             vaultURL + "/keys/my-key",
		KeySpec:           seaweedkms.KeySpecAES256,
		EncryptionContext: contextMap,
	})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	if sawAAD {
		t.Fatal("encrypt request carried AAD")
	}

	decrypted, err := provider.Decrypt(context.Background(), &seaweedkms.DecryptRequest{
		CiphertextBlob:    resp.CiphertextBlob,
		EncryptionContext: contextMap,
	})
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(decrypted.Plaintext, resp.Plaintext) {
		t.Fatal("decrypted key differs from generated key")
	}

	if _, err := provider.Decrypt(context.Background(), &seaweedkms.DecryptRequest{
		CiphertextBlob:    resp.CiphertextBlob,
		EncryptionContext: map[string]string{"aws:s3:bucket": "other"},
	}); err == nil {
		t.Fatal("Decrypt succeeded with a different encryption context")
	}
}

func TestCheckContextEmpty(t *testing.T) {
	// Keys wrapped without a context carry no digest; decrypting them with
	// no context must succeed, and with a context must fail.
	blob, err := seaweedkms.CreateEnvelope("azure", "my-key", "d3JhcHBlZA==", nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	envelope, err := seaweedkms.ParseEnvelope(blob)
	if err != nil {
		t.Fatalf("parse envelope: %v", err)
	}
	for _, context := range []map[string]string{nil, {}} {
		if err := checkContext(envelope, context); err != nil {
			t.Fatalf("empty context rejected for an unbound key: %v", err)
		}
	}
	if err := checkContext(envelope, map[string]string{"k": "v"}); err == nil {
		t.Fatal("context accepted for an unbound key")
	}
}
