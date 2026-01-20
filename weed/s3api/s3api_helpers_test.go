package s3api

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func TestIsPublicReadGrants(t *testing.T) {
	allUsers := s3_constants.GranteeGroupAllUsers
	authenticatedUsers := s3_constants.GranteeGroupAuthenticatedUsers
	readPermission := s3_constants.PermissionRead
	fullControlPermission := s3_constants.PermissionFullControl
	writePermission := s3_constants.PermissionWrite
	grantTypeGroup := s3_constants.GrantTypeGroup
	grantTypeCanonicalUser := s3_constants.GrantTypeCanonicalUser
	canonicalId := "some-canonical-id"

	tests := []struct {
		name     string
		grants   []*s3.Grant
		expected bool
	}{
		{
			name:     "Empty grants",
			grants:   []*s3.Grant{},
			expected: false,
		},
		{
			name: "Public read grant",
			grants: []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &grantTypeGroup,
						URI:  &allUsers,
					},
					Permission: &readPermission,
				},
			},
			expected: true,
		},
		{
			name: "Public full control grant",
			grants: []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &grantTypeGroup,
						URI:  &allUsers,
					},
					Permission: &fullControlPermission,
				},
			},
			expected: true,
		},
		{
			name: "Public write grant (not read)",
			grants: []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &grantTypeGroup,
						URI:  &allUsers,
					},
					Permission: &writePermission,
				},
			},
			expected: false,
		},
		{
			name: "Authenticated users grant (not public)",
			grants: []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &grantTypeGroup,
						URI:  &authenticatedUsers,
					},
					Permission: &readPermission,
				},
			},
			expected: false,
		},
		{
			name: "Specific user grant (not public)",
			grants: []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &grantTypeCanonicalUser,
						ID:   &canonicalId,
					},
					Permission: &readPermission,
				},
			},
			expected: false,
		},
		{
			name: "Multiple grants with public read",
			grants: []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &grantTypeCanonicalUser,
						ID:   &canonicalId,
					},
					Permission: &readPermission,
				},
				{
					Grantee: &s3.Grantee{
						Type: &grantTypeGroup,
						URI:  &allUsers,
					},
					Permission: &readPermission,
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isPublicReadGrants(tt.grants))
		})
	}
}


