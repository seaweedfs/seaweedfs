package policy_engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConvertSingleActionDeleteObject tests support for s3:DeleteObject action (Issue #7864)
func TestConvertSingleActionDeleteObject(t *testing.T) {
	// Test that Write action includes DeleteObject S3 action
	stmt, err := convertSingleAction("Write:bucket")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)

	// Check that s3:DeleteObject is included in the actions
	actions := stmt.Action.Strings()
	assert.Contains(t, actions, "s3:DeleteObject", "Write action should include s3:DeleteObject")
	assert.Contains(t, actions, "s3:PutObject", "Write action should include s3:PutObject")
}

// TestConvertSingleActionSubpath tests subpath handling for legacy actions (Issue #7864)
func TestConvertSingleActionSubpath(t *testing.T) {
	testCases := []struct {
		name              string
		action            string
		expectedActions   []string
		expectedResources []string
		description       string
	}{
		{
			name:              "Write_on_bucket",
			action:            "Write:mybucket",
			expectedActions:   []string{"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
			expectedResources: []string{"arn:aws:s3:::mybucket/*"},
			description:       "Write permission on bucket should create ARN for all objects in bucket",
		},
		{
			name:              "Write_on_bucket_with_wildcard",
			action:            "Write:mybucket/*",
			expectedActions:   []string{"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
			expectedResources: []string{"arn:aws:s3:::mybucket/*"},
			description:       "Write permission with /* should create ARN for all objects",
		},
		{
			name:              "Write_on_subpath",
			action:            "Write:mybucket/sub_path/*",
			expectedActions:   []string{"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
			expectedResources: []string{"arn:aws:s3:::mybucket/sub_path/*"},
			description:       "Write permission on subpath should restrict to objects under that path",
		},
		{
			name:              "Read_on_subpath",
			action:            "Read:mybucket/documents/*",
			expectedActions:   []string{"s3:GetObject", "s3:GetObjectVersion"},
			expectedResources: []string{"arn:aws:s3:::mybucket/documents/*"},
			description:       "Read permission on subpath should restrict to subpath objects",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := convertSingleAction(tc.action)
			assert.NoError(t, err, tc.description)
			assert.NotNil(t, stmt)

			// Check actions
			actions := stmt.Action.Strings()
			for _, expectedAction := range tc.expectedActions {
				assert.Contains(t, actions, expectedAction,
					"Action %s should be included for %s", expectedAction, tc.action)
			}

			// Check resources - verify all expected resources are present
			resources := stmt.Resource.Strings()
			assert.ElementsMatch(t, resources, tc.expectedResources,
				"Resources should match exactly for %s. Got %v, expected %v", tc.action, resources, tc.expectedResources)
		})
	}
}

// TestConvertSingleActionSubpathDeleteAllowed tests that DeleteObject works on subpaths
func TestConvertSingleActionSubpathDeleteAllowed(t *testing.T) {
	// This test specifically addresses Issue #7864 part 1:
	// "when a user is granted permission to a subpath, eg s3.configure -user someuser
	//  -actions Write -buckets some_bucket/sub_path/* -apply
	//  the user will only be able to put, but not delete object under somebucket/sub_path"

	stmt, err := convertSingleAction("Write:some_bucket/sub_path/*")
	assert.NoError(t, err)

	// The fix: s3:DeleteObject should be in the allowed actions
	actions := stmt.Action.Strings()
	assert.Contains(t, actions, "s3:DeleteObject",
		"Write permission on subpath should allow deletion of objects in that path")

	// The resource should be restricted to the subpath
	resources := stmt.Resource.Strings()
	assert.Contains(t, resources, "arn:aws:s3:::some_bucket/sub_path/*",
		"Delete permission should apply to objects under the subpath")
}

// TestConvertSingleActionNestedPaths tests deeply nested paths
func TestConvertSingleActionNestedPaths(t *testing.T) {
	testCases := []struct {
		action            string
		expectedResources []string
	}{
		{
			action:            "Write:bucket/a/b/c/*",
			expectedResources: []string{"arn:aws:s3:::bucket/a/b/c/*"},
		},
		{
			action:            "Read:bucket/data/documents/2024/*",
			expectedResources: []string{"arn:aws:s3:::bucket/data/documents/2024/*"},
		},
	}

	for _, tc := range testCases {
		stmt, err := convertSingleAction(tc.action)
		assert.NoError(t, err)

		resources := stmt.Resource.Strings()
		assert.ElementsMatch(t, resources, tc.expectedResources)
	}
}

// TestGetResourcesFromLegacyAction tests that GetResourcesFromLegacyAction generates
// action-appropriate resources consistent with convertSingleAction
func TestGetResourcesFromLegacyAction(t *testing.T) {
	testCases := []struct {
		name              string
		action            string
		expectedResources []string
		description       string
	}{
		// List actions - bucket-only (no object ARNs)
		{
			name:              "List_on_bucket",
			action:            "List:mybucket",
			expectedResources: []string{"arn:aws:s3:::mybucket"},
			description:       "List action should only have bucket ARN",
		},
		{
			name:              "List_on_bucket_with_wildcard",
			action:            "List:mybucket/*",
			expectedResources: []string{"arn:aws:s3:::mybucket"},
			description:       "List action should only have bucket ARN regardless of wildcard",
		},
		// Read actions - object-level ARNs only (s3:ListBucket was removed)
		{
			name:              "Read_on_bucket",
			action:            "Read:mybucket",
			expectedResources: []string{"arn:aws:s3:::mybucket/*"},
			description:       "Read action should have object-level ARN for all objects in bucket",
		},
		{
			name:              "Read_on_subpath",
			action:            "Read:mybucket/documents/*",
			expectedResources: []string{"arn:aws:s3:::mybucket/documents/*"},
			description:       "Read action on subpath should have object-level ARN for subpath only",
		},
		// Write actions - object-only for subpaths
		{
			name:              "Write_on_subpath",
			action:            "Write:mybucket/sub_path/*",
			expectedResources: []string{"arn:aws:s3:::mybucket/sub_path/*"},
			description:       "Write action on subpath should only have object ARN",
		},
		// Admin actions - both bucket and object ARNs
		{
			name:              "Admin_on_bucket",
			action:            "Admin:mybucket",
			expectedResources: []string{"arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/*"},
			description:       "Admin action should have both bucket and object ARNs",
		},
		{
			name:              "Admin_on_subpath",
			action:            "Admin:mybucket/admin/section/*",
			expectedResources: []string{"arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/admin/section/*"},
			description:       "Admin action on subpath should restrict to subpath, preventing privilege escalation",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resources, err := GetResourcesFromLegacyAction(tc.action)
			assert.NoError(t, err, tc.description)
			assert.ElementsMatch(t, resources, tc.expectedResources,
				"Resources should match expected. Got %v, expected %v", resources, tc.expectedResources)

			// Also verify consistency with convertSingleAction where applicable
			stmt, err := convertSingleAction(tc.action)
			assert.NoError(t, err)

			stmtResources := stmt.Resource.Strings()
			assert.ElementsMatch(t, resources, stmtResources,
				"GetResourcesFromLegacyAction should match convertSingleAction resources for %s", tc.action)
		})
	}
}

// TestExtractBucketAndPrefixEdgeCases validates edge case handling in extractBucketAndPrefix
func TestExtractBucketAndPrefixEdgeCases(t *testing.T) {
	testCases := []struct {
		name           string
		pattern        string
		expectedBucket string
		expectedPrefix string
		description    string
	}{
		{
			name:           "Empty string",
			pattern:        "",
			expectedBucket: "",
			expectedPrefix: "",
			description:    "Empty pattern should return empty strings",
		},
		{
			name:           "Whitespace only",
			pattern:        "   ",
			expectedBucket: "",
			expectedPrefix: "",
			description:    "Whitespace-only pattern should return empty strings",
		},
		{
			name:           "Slash only",
			pattern:        "/",
			expectedBucket: "",
			expectedPrefix: "",
			description:    "Slash-only pattern should return empty strings",
		},
		{
			name:           "Double slash prefix",
			pattern:        "bucket//prefix/*",
			expectedBucket: "bucket",
			expectedPrefix: "prefix",
			description:    "Double slash should be normalized (trailing slashes removed)",
		},
		{
			name:           "Normal bucket",
			pattern:        "mybucket",
			expectedBucket: "mybucket",
			expectedPrefix: "",
			description:    "Bucket-only pattern should work correctly",
		},
		{
			name:           "Bucket with prefix",
			pattern:        "mybucket/myprefix/*",
			expectedBucket: "mybucket",
			expectedPrefix: "myprefix",
			description:    "Bucket with prefix should be parsed correctly",
		},
		{
			name:           "Nested prefix",
			pattern:        "mybucket/a/b/c/*",
			expectedBucket: "mybucket",
			expectedPrefix: "a/b/c",
			description:    "Nested prefix should be preserved",
		},
		{
			name:           "Bucket with trailing slash",
			pattern:        "mybucket/",
			expectedBucket: "mybucket",
			expectedPrefix: "",
			description:    "Trailing slash on bucket should be normalized",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, prefix := extractBucketAndPrefix(tc.pattern)
			assert.Equal(t, tc.expectedBucket, bucket, tc.description)
			assert.Equal(t, tc.expectedPrefix, prefix, tc.description)
		})
	}
}

// TestCreatePolicyFromLegacyIdentityMultipleActions validates correct resource ARN aggregation
// when multiple action types target the same resource pattern
func TestCreatePolicyFromLegacyIdentityMultipleActions(t *testing.T) {
	testCases := []struct {
		name                     string
		identityName             string
		actions                  []string
		expectedStatements       int
		expectedActionsInStmt1   []string
		expectedResourcesInStmt1 []string
		description              string
	}{
		{
			name:               "List_and_Write_on_subpath",
			identityName:       "data-manager",
			actions:            []string{"List:mybucket/data/*", "Write:mybucket/data/*"},
			expectedStatements: 1,
			expectedActionsInStmt1: []string{
				"s3:ListBucket", "s3:ListBucketVersions",
				"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl",
			},
			expectedResourcesInStmt1: []string{
				"arn:aws:s3:::mybucket",        // From List action
				"arn:aws:s3:::mybucket/data/*", // From Write action
			},
			description: "List + Write on same subpath should aggregate both bucket and object ARNs",
		},
		{
			name:               "Read_and_Tagging_on_bucket",
			identityName:       "tag-reader",
			actions:            []string{"Read:mybucket", "Tagging:mybucket"},
			expectedStatements: 1,
			expectedActionsInStmt1: []string{
				"s3:GetObject", "s3:GetObjectVersion",
				"s3:GetObjectTagging", "s3:PutObjectTagging", "s3:DeleteObjectTagging",
			},
			expectedResourcesInStmt1: []string{
				"arn:aws:s3:::mybucket/*",
			},
			description: "Read + Tagging on same bucket should aggregate object-level ARNs only",
		},
		{
			name:                   "Admin_with_other_actions",
			identityName:           "admin-user",
			actions:                []string{"Admin:mybucket/admin/*", "Write:mybucket/admin/*"},
			expectedStatements:     1,
			expectedActionsInStmt1: []string{"s3:*"},
			expectedResourcesInStmt1: []string{
				"arn:aws:s3:::mybucket",
				"arn:aws:s3:::mybucket/admin/*",
			},
			description: "Admin action should dominate and set s3:*, other actions still processed for resources",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := CreatePolicyFromLegacyIdentity(tc.identityName, tc.actions)
			assert.NoError(t, err, tc.description)
			assert.NotNil(t, policy)

			// Check statement count
			assert.Equal(t, tc.expectedStatements, len(policy.Statement),
				"Expected %d statement(s), got %d", tc.expectedStatements, len(policy.Statement))

			if tc.expectedStatements > 0 {
				stmt := policy.Statement[0]

				// Check actions
				actualActions := stmt.Action.Strings()
				for _, expectedAction := range tc.expectedActionsInStmt1 {
					assert.Contains(t, actualActions, expectedAction,
						"Action %s should be included in statement", expectedAction)
				}

				// Check resources - all expected resources should be present
				actualResources := stmt.Resource.Strings()
				assert.ElementsMatch(t, tc.expectedResourcesInStmt1, actualResources,
					"Statement should aggregate all required resource ARNs. Got %v, expected %v",
					actualResources, tc.expectedResourcesInStmt1)
			}
		})
	}
}
