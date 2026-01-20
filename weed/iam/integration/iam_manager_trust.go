package integration

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
)

// ValidateTrustPolicyForPrincipal validates if a principal is allowed to assume a role
func (m *IAMManager) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(roleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return fmt.Errorf("failed to get role %s: %w", roleName, err)
	}

	if roleDef.TrustPolicy == nil {
		return fmt.Errorf("role has no trust policy")
	}

	// Extract username from principal ARN
	// Format: arn:aws:iam::seaweedfs:user/username
	parts := strings.Split(principalArn, "/")
	userId := parts[len(parts)-1]

	// Create evaluation context
	evalCtx := &policy.EvaluationContext{
		Principal: principalArn,
		Action:    "sts:AssumeRole",
		Resource:  roleDef.RoleArn,
		RequestContext: map[string]interface{}{
			"aws:PrincipalArn":     principalArn,
			"aws:username":         userId,
			"seaweed:AWSPrincipal": principalArn,
		},
	}

	// Evaluate the trust policy
	if !m.evaluateTrustPolicy(roleDef.TrustPolicy, evalCtx) {
		return fmt.Errorf("trust policy denies access to principal: %s", principalArn)
	}

	return nil
}
