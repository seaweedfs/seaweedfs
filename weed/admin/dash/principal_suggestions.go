package dash

import (
	"context"

	weediam "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// principalRoleArn builds the ARN SeaweedFS assigns a role by default (when
// its RoleDefinition.RoleArn isn't explicitly set) - see
// weed/iam/integration/iam_manager.go's CreateRole. ListRoles only returns
// role names, so this reconstructs the well-known default rather than
// fetching every role's stored definition just to populate a suggestion list.
func principalRoleArn(roleName string) string {
	return "arn:aws:iam::role/" + roleName
}

// GetPrincipalSuggestions returns candidate ARNs for the policy editor's
// Principal/NotPrincipal autocomplete: one per S3 user, plus one per IAM
// role. Service accounts are deliberately not listed separately - a service
// account is just an additional credential for its parent user, so its ARN
// is identical to the one already suggested for that user.
//
// Role listing is best-effort: if the filer or role store is unavailable,
// the error is logged and suggestions fall back to users only, since an
// incomplete autocomplete list is far less disruptive than blocking policy
// editing over a suggestions-only feature.
func (s *AdminServer) GetPrincipalSuggestions(ctx context.Context) ([]string, error) {
	var suggestions []string

	users, err := s.GetObjectStoreUsers(ctx)
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		suggestions = append(suggestions, weediam.UserArn(u.Username))
	}

	roleStore, err := integration.NewFilerRoleStore(nil, func() string { return s.GetFilerAddress() })
	if err != nil {
		glog.Warningf("GetPrincipalSuggestions: failed to create role store: %v", err)
		return suggestions, nil
	}
	roleNames, err := roleStore.ListRoles(ctx, s.GetFilerAddress())
	if err != nil {
		glog.Warningf("GetPrincipalSuggestions: failed to list roles: %v", err)
		return suggestions, nil
	}
	for _, roleName := range roleNames {
		suggestions = append(suggestions, principalRoleArn(roleName))
	}

	return suggestions, nil
}
