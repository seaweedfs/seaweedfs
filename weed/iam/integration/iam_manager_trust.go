package integration

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
)

// ErrTrustPolicyDenied is wrapped when a role's trust policy does not admit
// the principal.
var ErrTrustPolicyDenied = errors.New("trust policy denies access to principal")

// ValidateTrustPolicyForPrincipal validates if a principal is allowed to assume a role
func (m *IAMManager) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	_, err := m.ResolveRoleForPrincipal(ctx, roleArn, principalArn)
	return err
}

// ResolveRoleForPrincipal returns the role roleArn names if its trust policy
// admits principalArn. Issuing a session from the definition returned binds
// the session to the role whose trust was evaluated, not to a role that
// replaced it under the same name in between.
func (m *IAMManager) ResolveRoleForPrincipal(ctx context.Context, roleArn, principalArn string) (*RoleDefinition, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(roleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get role %s: %w", roleName, err)
	}

	if roleDef.TrustPolicy == nil {
		return nil, fmt.Errorf("%w: role has no trust policy", ErrTrustPolicyDenied)
	}

	// Create evaluation context with RequestContext populated so that
	// principal matching works for specific (non-wildcard) principals.
	// Without this, evaluatePrincipalValue cannot look up "aws:PrincipalArn"
	// and always returns false for non-wildcard trust policy principals.
	evalCtx := &policy.EvaluationContext{
		Principal: principalArn,
		Action:    "sts:AssumeRole",
		Resource:  roleArn,
		RequestContext: map[string]interface{}{
			"principal":        principalArn,
			"aws:PrincipalArn": principalArn,
		},
	}

	// Evaluate the trust policy
	if !m.evaluateTrustPolicy(roleDef.TrustPolicy, evalCtx) {
		return nil, fmt.Errorf("%w: %s", ErrTrustPolicyDenied, principalArn)
	}

	return roleDef, nil
}
