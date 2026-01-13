package s3api

import (
	"context"
	"fmt"
)

// ValidateTrustPolicyForPrincipal validates if a principal is allowed to assume a role
// Delegates to the IAM integration if available
func (iam *IdentityAccessManagement) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	if iam.iamIntegration != nil {
		return iam.iamIntegration.ValidateTrustPolicyForPrincipal(ctx, roleArn, principalArn)
	}
	return fmt.Errorf("IAM integration not available")
}
