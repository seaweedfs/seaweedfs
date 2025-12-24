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
		name             string
		action           string
		bucket           string
		expectedActions  []string
		expectedResource string
		description      string
	}{
		{
			name:             "Write_on_bucket",
			action:           "Write:mybucket",
			bucket:           "mybucket",
			expectedActions:  []string{"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
			expectedResource: "arn:aws:s3:::mybucket",
			description:      "Write permission on bucket should create ARN for bucket itself",
		},
		{
			name:             "Write_on_bucket_with_wildcard",
			action:           "Write:mybucket/*",
			bucket:           "mybucket",
			expectedActions:  []string{"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
			expectedResource: "arn:aws:s3:::mybucket/*",
			description:      "Write permission with /* should create ARN for all objects",
		},
		{
			name:             "Write_on_subpath",
			action:           "Write:mybucket/sub_path/*",
			bucket:           "mybucket",
			expectedActions:  []string{"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
			expectedResource: "arn:aws:s3:::mybucket/sub_path/*",
			description:      "Write permission on subpath should restrict to objects under that path",
		},
		{
			name:             "Read_on_subpath",
			action:           "Read:mybucket/documents/*",
			bucket:           "mybucket",
			expectedActions:  []string{"s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"},
			expectedResource: "arn:aws:s3:::mybucket/documents/*",
			description:      "Read permission on subpath should restrict to objects under that path",
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

			// Check resources
			resources := stmt.Resource.Strings()
			assert.Contains(t, resources, tc.expectedResource,
				"Resource %s should be in the statement for %s", tc.expectedResource, tc.action)
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
		action           string
		expectedResource string
	}{
		{
			action:           "Write:bucket/a/b/c/*",
			expectedResource: "arn:aws:s3:::bucket/a/b/c/*",
		},
		{
			action:           "Read:bucket/data/documents/2024/*",
			expectedResource: "arn:aws:s3:::bucket/data/documents/2024/*",
		},
	}

	for _, tc := range testCases {
		stmt, err := convertSingleAction(tc.action)
		assert.NoError(t, err)

		resources := stmt.Resource.Strings()
		assert.Contains(t, resources, tc.expectedResource)
	}
}
