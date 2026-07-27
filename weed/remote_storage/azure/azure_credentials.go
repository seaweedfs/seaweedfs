package azure

import (
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// NewAzBlobClient builds a blob service client for accountName.
//
// An empty accountKey selects Entra ID instead of a shared key: azidentity
// resolves a workload identity, a managed identity, or a developer login from
// the environment, and access is granted through RBAC. Fleets that cannot
// distribute and rotate storage account keys authenticate that way. clientID
// pins a user-assigned identity when the environment offers more than one.
func NewAzBlobClient(accountName, accountKey, clientID string) (*azblob.Client, error) {

	if accountName == "" {
		return nil, fmt.Errorf("azure account name is required")
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

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
