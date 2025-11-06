package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/stretchr/testify/assert"
)

func TestSelectPrimaryRole(t *testing.T) {
	s3iam := &S3IAMIntegration{}

	t.Run("empty_roles_returns_empty", func(t *testing.T) {
		identity := &providers.ExternalIdentity{Attributes: make(map[string]string)}
		result := s3iam.selectPrimaryRole([]string{}, identity)
		assert.Equal(t, "", result)
	})

	t.Run("single_role_returns_that_role", func(t *testing.T) {
		identity := &providers.ExternalIdentity{Attributes: make(map[string]string)}
		result := s3iam.selectPrimaryRole([]string{"admin"}, identity)
		assert.Equal(t, "admin", result)
	})

	t.Run("multiple_roles_returns_first", func(t *testing.T) {
		identity := &providers.ExternalIdentity{Attributes: make(map[string]string)}
		roles := []string{"viewer", "manager", "admin"}
		result := s3iam.selectPrimaryRole(roles, identity)
		assert.Equal(t, "viewer", result, "Should return first role")
	})

	t.Run("order_matters", func(t *testing.T) {
		identity := &providers.ExternalIdentity{Attributes: make(map[string]string)}

		// Test different orderings
		roles1 := []string{"admin", "viewer", "manager"}
		result1 := s3iam.selectPrimaryRole(roles1, identity)
		assert.Equal(t, "admin", result1)

		roles2 := []string{"viewer", "admin", "manager"}
		result2 := s3iam.selectPrimaryRole(roles2, identity)
		assert.Equal(t, "viewer", result2)

		roles3 := []string{"manager", "admin", "viewer"}
		result3 := s3iam.selectPrimaryRole(roles3, identity)
		assert.Equal(t, "manager", result3)
	})

	t.Run("complex_enterprise_roles", func(t *testing.T) {
		identity := &providers.ExternalIdentity{Attributes: make(map[string]string)}
		roles := []string{
			"finance-readonly",
			"hr-manager",
			"it-system-admin",
			"guest-viewer",
		}
		result := s3iam.selectPrimaryRole(roles, identity)
		// Should return the first role
		assert.Equal(t, "finance-readonly", result, "Should return first role in list")
	})
}
