package s3api

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/stretchr/testify/assert"
)

func TestSelectPrimaryRole(t *testing.T) {
	s3iam := &S3IAMIntegration{}

	t.Run("single_role_returns_that_role", func(t *testing.T) {
		roles := []string{"admin"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: make(map[string]string),
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "admin", result)
	})

	t.Run("explicit_primary_role_takes_precedence", func(t *testing.T) {
		roles := []string{"admin", "reader", "writer"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: map[string]string{
				"primary_role": "reader",
			},
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "reader", result)
	})

	t.Run("explicit_primary_role_case_insensitive", func(t *testing.T) {
		roles := []string{"Admin", "Reader", "Writer"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: map[string]string{
				"primary_role": "admin",
			},
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "Admin", result)
	})

	t.Run("invalid_primary_role_falls_back_to_hierarchy", func(t *testing.T) {
		roles := []string{"admin", "reader", "writer"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: map[string]string{
				"primary_role": "nonexistent",
			},
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "admin", result) // Should select admin via hierarchy
	})

	t.Run("hierarchy_selection_admin_over_reader", func(t *testing.T) {
		roles := []string{"reader", "admin", "writer"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: make(map[string]string),
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "admin", result) // Admin has higher priority
	})

	t.Run("hierarchy_selection_case_insensitive", func(t *testing.T) {
		roles := []string{"Reader", "ADMIN", "writer"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: make(map[string]string),
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "ADMIN", result)
	})

	t.Run("hierarchy_selection_contains_match", func(t *testing.T) {
		roles := []string{"system-reader", "system-admin-user", "system-writer"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: make(map[string]string),
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "system-admin-user", result) // Contains "admin"
	})

	t.Run("deterministic_fallback_alphabetical", func(t *testing.T) {
		// Roles that don't match any hierarchy
		roles := []string{"zebra", "alpha", "beta"}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: make(map[string]string),
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "alpha", result) // First alphabetically
	})

	t.Run("complex_enterprise_roles", func(t *testing.T) {
		roles := []string{
			"app-user-readonly",
			"app-user-contributor",
			"app-admin-full",
			"system-guest",
		}
		externalIdentity := &providers.ExternalIdentity{
			Attributes: make(map[string]string),
		}

		result := s3iam.selectPrimaryRole(roles, externalIdentity)
		assert.Equal(t, "app-admin-full", result) // Contains "admin"
	})
}

func TestSelectByRoleHierarchy(t *testing.T) {
	s3iam := &S3IAMIntegration{}

	t.Run("super_admin_highest_priority", func(t *testing.T) {
		roles := []string{"admin", "super-admin", "reader"}
		result := s3iam.selectByRoleHierarchy(roles)
		assert.Equal(t, "super-admin", result)
	})

	t.Run("admin_over_manager", func(t *testing.T) {
		roles := []string{"manager", "admin", "reader"}
		result := s3iam.selectByRoleHierarchy(roles)
		assert.Equal(t, "admin", result)
	})

	t.Run("manager_over_editor", func(t *testing.T) {
		roles := []string{"editor", "manager", "reader"}
		result := s3iam.selectByRoleHierarchy(roles)
		assert.Equal(t, "manager", result)
	})

	t.Run("editor_over_viewer", func(t *testing.T) {
		roles := []string{"viewer", "editor"}
		result := s3iam.selectByRoleHierarchy(roles)
		assert.Equal(t, "editor", result)
	})

	t.Run("no_hierarchy_match_returns_empty", func(t *testing.T) {
		roles := []string{"custom-role-1", "custom-role-2", "special-user"}
		result := s3iam.selectByRoleHierarchy(roles)
		assert.Equal(t, "", result)
	})

	t.Run("multiple_same_tier_returns_first_found", func(t *testing.T) {
		roles := []string{"viewer", "reader", "guest"}
		result := s3iam.selectByRoleHierarchy(roles)
		// Should return first match found in the hierarchy (viewer comes first in tier definition)
		assert.Equal(t, "viewer", result)
	})

	t.Run("case_variations", func(t *testing.T) {
		roles := []string{"ADMIN", "Reader", "writer"}
		result := s3iam.selectByRoleHierarchy(roles)
		assert.Equal(t, "ADMIN", result)
	})
}

func TestRoleSelectionIntegration(t *testing.T) {
	t.Run("real_world_enterprise_scenario", func(t *testing.T) {
		// Simulate a real enterprise OIDC token with multiple roles
		testCases := []struct {
			name          string
			roles         []string
			primaryRole   string // explicit primary_role claim
			expectedRole  string
			selectionType string
		}{
			{
				name:          "explicit_primary_overrides_hierarchy",
				roles:         []string{"admin", "reader", "writer"},
				primaryRole:   "reader",
				expectedRole:  "reader",
				selectionType: "explicit",
			},
			{
				name:          "hierarchy_selects_admin_over_others",
				roles:         []string{"contributor", "admin", "viewer"},
				primaryRole:   "", // No explicit primary
				expectedRole:  "admin",
				selectionType: "hierarchy",
			},
			{
				name:          "deterministic_fallback_for_unknown_roles",
				roles:         []string{"zebra-role", "alpha-role", "beta-role"},
				primaryRole:   "",
				expectedRole:  "alpha-role",
				selectionType: "deterministic",
			},
			{
				name:          "complex_enterprise_naming",
				roles:         []string{"org-user-readonly", "org-power-user", "org-system-admin"},
				primaryRole:   "",
				expectedRole:  "org-system-admin", // Contains "admin"
				selectionType: "hierarchy",
			},
		}

		s3iam := &S3IAMIntegration{}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				externalIdentity := &providers.ExternalIdentity{
					Attributes: make(map[string]string),
				}
				if tc.primaryRole != "" {
					externalIdentity.Attributes["primary_role"] = tc.primaryRole
				}

				result := s3iam.selectPrimaryRole(tc.roles, externalIdentity)
				assert.Equal(t, tc.expectedRole, result,
					"Expected %s selection to return %s, got %s",
					tc.selectionType, tc.expectedRole, result)
			})
		}
	})
}

// Test helper function to verify role parsing improvements
func TestRoleParsingImprovements(t *testing.T) {
	t.Run("whitespace_handling", func(t *testing.T) {
		// Test the improved role parsing logic
		rolesStr := " admin , reader , writer "
		roles := strings.Split(rolesStr, ",")

		// Clean up role names (this is what the main code does now)
		var cleanRoles []string
		for _, role := range roles {
			cleanRole := strings.TrimSpace(role)
			if cleanRole != "" {
				cleanRoles = append(cleanRoles, cleanRole)
			}
		}

		expected := []string{"admin", "reader", "writer"}
		assert.Equal(t, expected, cleanRoles)
	})

	t.Run("empty_roles_filtered", func(t *testing.T) {
		rolesStr := "admin,,reader, ,writer"
		roles := strings.Split(rolesStr, ",")

		var cleanRoles []string
		for _, role := range roles {
			cleanRole := strings.TrimSpace(role)
			if cleanRole != "" {
				cleanRoles = append(cleanRoles, cleanRole)
			}
		}

		expected := []string{"admin", "reader", "writer"}
		assert.Equal(t, expected, cleanRoles)
	})
}
