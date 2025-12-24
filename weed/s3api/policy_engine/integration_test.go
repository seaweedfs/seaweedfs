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
			expectedActions:   []string{"s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"},
			expectedResources: []string{"arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/documents/*"},
			description:       "Read permission on subpath should include bucket and subpath ARNs",
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
			expectedResources: []string{"arn:aws:s3:::bucket", "arn:aws:s3:::bucket/data/documents/2024/*"},
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
		// Read actions - both bucket and object ARNs
		{
			name:              "Read_on_bucket",
			action:            "Read:mybucket",
			expectedResources: []string{"arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/*"},
			description:       "Read action should have both bucket and object ARNs",
		},
		{
			name:              "Read_on_subpath",
			action:            "Read:mybucket/documents/*",
			expectedResources: []string{"arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/documents/*"},
			description:       "Read action on subpath should have bucket and subpath ARNs",
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
