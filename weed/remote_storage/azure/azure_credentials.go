package azure

import (
	"fmt"
	"net/url"
	"os"
	"regexp"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Azure storage account names are 3 to 24 lowercase letters and digits. The
// name lands in the service URL, where a stray "/", "?" or "@" would move the
// authority somewhere else and send an authenticated request to that host.
var validAzureAccountName = regexp.MustCompile(`^[a-z0-9]{3,24}$`)

// NewAzBlobClient builds a blob service client for accountName.
//
// An empty accountKey selects Entra ID instead of a shared key: azidentity
// resolves a workload identity, a managed identity, or a developer login from
// the environment, and access is granted through RBAC. Fleets that cannot
// distribute and rotate storage account keys authenticate that way. clientID
// pins a user-assigned identity when the environment offers more than one.
//
// endpoint is the blob service URL, for accounts outside the public cloud. An
// empty endpoint derives the public one from accountName.
func NewAzBlobClient(accountName, accountKey, clientID, endpoint string) (*azblob.Client, error) {

	if accountName == "" {
		return nil, fmt.Errorf("azure account name is required")
	}
	if !validAzureAccountName.MatchString(accountName) {
		return nil, fmt.Errorf("invalid azure account name %q: expecting 3 to 24 lowercase letters and digits", accountName)
	}

	serviceURL, err := azureServiceURL(accountName, endpoint)
	if err != nil {
		return nil, err
	}

	if accountKey == "" {
		credential, err := newAzureTokenCredential(clientID)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Entra ID credential for account %s: %w", accountName, err)
		}
		glog.V(1).Infof("azure %s: authenticating with Entra ID", accountName)
		client, err := azblob.NewClient(serviceURL, credential, DefaultAzBlobClientOptions())
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client: %w", err)
		}
		return client, nil
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure credential with account name:%s: %w", accountName, err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, DefaultAzBlobClientOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure client: %w", err)
	}
	return client, nil
}

// azureServiceURL locates the blob service. Sovereign clouds and private
// endpoints do not live under blob.core.windows.net, so they name the service
// URL outright rather than having it derived from the account.
func azureServiceURL(accountName, endpoint string) (string, error) {
	if endpoint == "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net/", accountName), nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid azure endpoint %q: %w", endpoint, err)
	}
	// plain http would carry the account key or the bearer token in the clear
	if parsed.Scheme != "https" || parsed.Hostname() == "" {
		return "", fmt.Errorf("invalid azure endpoint %q: expecting an https service url, such as https://%s.blob.core.usgovcloudapi.net/", endpoint, accountName)
	}
	return endpoint, nil
}

// newAzureTokenCredential resolves an Entra ID credential. Without a pinned
// clientID the default chain discovers whatever the host offers. With one, the
// federated token file projected by the Azure workload identity webhook tells
// the two identity flavors apart.
func newAzureTokenCredential(clientID string) (azcore.TokenCredential, error) {
	if clientID == "" {
		return azidentity.NewDefaultAzureCredential(nil)
	}
	if os.Getenv("AZURE_FEDERATED_TOKEN_FILE") != "" {
		return azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID: clientID,
		})
	}
	return azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
		ID: azidentity.ClientID(clientID),
	})
}
