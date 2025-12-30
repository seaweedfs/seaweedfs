package dash

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// identityToServiceAccount converts an IAM identity to a ServiceAccount struct
// This helper reduces code duplication across GetServiceAccounts, GetServiceAccountDetails,
// UpdateServiceAccount, and GetServiceAccountByAccessKey
func identityToServiceAccount(identity *iam_pb.Identity) (*ServiceAccount, error) {
	if identity == nil {
		return nil, fmt.Errorf("identity cannot be nil")
	}
	if !strings.HasPrefix(identity.GetName(), serviceAccountPrefix) {
		return nil, fmt.Errorf("not a service account: %s", identity.GetName())
	}

	parts := strings.SplitN(identity.GetName(), ":", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid service account ID format")
	}

	sa := &ServiceAccount{
		ID:         identity.GetName(),
		ParentUser: parts[1],
		Status:     StatusActive,
		CreateDate: getCreationDate(identity.GetActions()),
		Expiration: getExpiration(identity.GetActions()),
	}

	// Get description from account display name
	if identity.Account != nil {
		sa.Description = identity.Account.GetDisplayName()
	}

	// Get access key from credentials
	if len(identity.Credentials) > 0 {
		sa.AccessKeyId = identity.Credentials[0].GetAccessKey()
	}

	// Check if disabled
	for _, action := range identity.GetActions() {
		if action == disabledAction {
			sa.Status = StatusInactive
			break
		}
	}

	return sa, nil
}
