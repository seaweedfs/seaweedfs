package azure

import (
	"path/filepath"
	"testing"
)

func TestNewAzBlobClientRequiresAccountName(t *testing.T) {
	if _, err := NewAzBlobClient("", "aW52YWxpZGtleQ==", "", ""); err == nil {
		t.Error("expected an error without an account name")
	}
}

func TestNewAzBlobClientRejectsMalformedAccountName(t *testing.T) {
	for _, accountName := range []string{"ab", "TestAccount", "test-account", "evil.com/x", "evil@host.com", "account?x=1"} {
		if _, err := NewAzBlobClient(accountName, "", "", ""); err == nil {
			t.Errorf("expected an error for account name %q", accountName)
		}
	}
}

func TestNewAzBlobClientSharedKey(t *testing.T) {
	client, err := NewAzBlobClient("testaccount", "aW52YWxpZGtleQ==", "", "")
	if err != nil {
		t.Fatalf("failed to create a shared key client: %v", err)
	}
	if client == nil {
		t.Error("expected a client")
	}
}

func TestNewAzBlobClientSharedKeyRejectsMalformedKey(t *testing.T) {
	if _, err := NewAzBlobClient("testaccount", "not base64", "", ""); err == nil {
		t.Error("expected an error with a malformed account key")
	}
}

func TestAzureServiceURL(t *testing.T) {
	serviceURL, err := azureServiceURL("testaccount", "")
	if err != nil {
		t.Fatalf("failed to derive the public service url: %v", err)
	}
	if serviceURL != "https://testaccount.blob.core.windows.net/" {
		t.Errorf("unexpected public service url %q", serviceURL)
	}

	government := "https://testaccount.blob.core.usgovcloudapi.net/"
	serviceURL, err = azureServiceURL("testaccount", government)
	if err != nil {
		t.Fatalf("failed to keep the configured service url: %v", err)
	}
	if serviceURL != government {
		t.Errorf("expected %q, got %q", government, serviceURL)
	}

	for _, endpoint := range []string{"core.usgovcloudapi.net", "http://testaccount.blob.core.windows.net/", "https://", "https://:443/", "://x"} {
		if _, err := azureServiceURL("testaccount", endpoint); err == nil {
			t.Errorf("expected an error for endpoint %q", endpoint)
		}
	}
}

// no account key: authenticate through the Entra ID chain instead
func TestNewAzBlobClientEntraID(t *testing.T) {
	client, err := NewAzBlobClient("testaccount", "", "", "")
	if err != nil {
		t.Fatalf("failed to create an Entra ID client: %v", err)
	}
	if client == nil {
		t.Error("expected a client")
	}
}

func TestNewAzBlobClientWorkloadIdentity(t *testing.T) {
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", filepath.Join(t.TempDir(), "token"))
	t.Setenv("AZURE_TENANT_ID", "00000000-0000-0000-0000-000000000000")

	client, err := NewAzBlobClient("testaccount", "", "11111111-1111-1111-1111-111111111111", "")
	if err != nil {
		t.Fatalf("failed to create a workload identity client: %v", err)
	}
	if client == nil {
		t.Error("expected a client")
	}
}

// a pinned client id without a projected token file is a plain managed identity
func TestNewAzureTokenCredentialManagedIdentity(t *testing.T) {
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "")

	credential, err := newAzureTokenCredential("11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("failed to create a managed identity credential: %v", err)
	}
	if credential == nil {
		t.Error("expected a credential")
	}
}
