package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckPolicyWithEntry tests the checkPolicyWithEntry helper method
// This verifies that tag-based conditions are properly evaluated when
// the object entry is available (Phase 2 of policy evaluation)
func TestCheckPolicyWithEntry(t *testing.T) {
	// Create a minimal S3ApiServer with policy engine
	s3a := &S3ApiServer{
		policyEngine: NewBucketPolicyEngine(),
	}

	bucket := "test-bucket"

	// Set up a policy that allows access only to objects with status=public tag
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*",
				"Condition": {
					"StringEquals": {
						"s3:ExistingObjectTag/status": ["public"]
					}
				}
			}
		]
	}`

	err := s3a.policyEngine.engine.SetBucketPolicy(bucket, policyJSON)
	require.NoError(t, err)

	tests := []struct {
		name        string
		objectEntry map[string][]byte
		wantErrCode s3err.ErrorCode
		wantChecked bool
		description string
	}{
		{
			name: "object with public tag - allow",
			objectEntry: map[string][]byte{
				s3_constants.AmzObjectTaggingPrefix + "status": []byte("public"),
			},
			wantErrCode: s3err.ErrNone,
			wantChecked: true,
			description: "Object with status=public tag should be allowed",
		},
		{
			name: "object with private tag - indeterminate (no matching statement)",
			objectEntry: map[string][]byte{
				s3_constants.AmzObjectTaggingPrefix + "status": []byte("private"),
			},
			wantErrCode: s3err.ErrNone,
			wantChecked: false, // Indeterminate = no policy matched
			description: "Object with status=private should return indeterminate (falls back to IAM)",
		},
		{
			name:        "object with no tags - indeterminate (no matching statement)",
			objectEntry: map[string][]byte{},
			wantErrCode: s3err.ErrNone,
			wantChecked: false, // Indeterminate = no policy matched
			description: "Object without tags should return indeterminate (falls back to IAM)",
		},
		{
			name:        "nil entry - indeterminate (condition cannot be evaluated)",
			objectEntry: nil,
			wantErrCode: s3err.ErrNone,
			wantChecked: false, // Indeterminate = no policy matched
			description: "Nil entry should return indeterminate (falls back to IAM)",
		},
		{
			name: "object with multiple tags including public - allow",
			objectEntry: map[string][]byte{
				s3_constants.AmzObjectTaggingPrefix + "status":   []byte("public"),
				s3_constants.AmzObjectTaggingPrefix + "category": []byte("documents"),
			},
			wantErrCode: s3err.ErrNone,
			wantChecked: true,
			description: "Object with status=public among other tags should be allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test-bucket/test-object.txt", nil)
			object := "/test-object.txt"
			principal := "*"
			action := string(s3_constants.ACTION_READ)

			errCode, checked := s3a.checkPolicyWithEntry(req, bucket, object, action, principal, tt.objectEntry)

			assert.Equal(t, tt.wantErrCode, errCode, tt.description)
			assert.Equal(t, tt.wantChecked, checked, "Policy should be evaluated")
		})
	}
}

// TestCheckPolicyWithEntryNoPolicyForBucket tests that checkPolicyWithEntry
// returns early when there's no policy for the bucket
func TestCheckPolicyWithEntryNoPolicyForBucket(t *testing.T) {
	s3a := &S3ApiServer{
		policyEngine: NewBucketPolicyEngine(),
	}

	bucket := "bucket-without-policy"
	req := httptest.NewRequest(http.MethodGet, "/bucket-without-policy/test.txt", nil)

	objectEntry := map[string][]byte{
		s3_constants.AmzObjectTaggingPrefix + "status": []byte("private"),
	}

	errCode, checked := s3a.checkPolicyWithEntry(req, bucket, "/test.txt", string(s3_constants.ACTION_READ), "*", objectEntry)

	assert.Equal(t, s3err.ErrNone, errCode, "No policy should mean no denial")
	assert.False(t, checked, "Policy should not be evaluated when bucket has no policy")
}

// TestCheckPolicyWithEntryNilPolicyEngine tests that checkPolicyWithEntry
// handles nil policy engine gracefully
func TestCheckPolicyWithEntryNilPolicyEngine(t *testing.T) {
	s3a := &S3ApiServer{
		policyEngine: nil,
	}

	req := httptest.NewRequest(http.MethodGet, "/test-bucket/test.txt", nil)

	errCode, checked := s3a.checkPolicyWithEntry(req, "test-bucket", "/test.txt", string(s3_constants.ACTION_READ), "*", nil)

	assert.Equal(t, s3err.ErrNone, errCode, "Nil policy engine should allow access")
	assert.False(t, checked, "Policy should not be evaluated when engine is nil")
}

// TestCheckPolicyWithEntryDenyPolicy tests deny policies with tag conditions
func TestCheckPolicyWithEntryDenyPolicy(t *testing.T) {
	s3a := &S3ApiServer{
		policyEngine: NewBucketPolicyEngine(),
	}

	bucket := "secure-bucket"

	// Policy that allows all access but denies access to confidential objects
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::secure-bucket/*"
			},
			{
				"Effect": "Deny",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::secure-bucket/*",
				"Condition": {
					"StringEquals": {
						"s3:ExistingObjectTag/classification": ["confidential"]
					}
				}
			}
		]
	}`

	err := s3a.policyEngine.engine.SetBucketPolicy(bucket, policyJSON)
	require.NoError(t, err)

	tests := []struct {
		name        string
		objectEntry map[string][]byte
		wantErrCode s3err.ErrorCode
		description string
	}{
		{
			name: "public object - allow",
			objectEntry: map[string][]byte{
				s3_constants.AmzObjectTaggingPrefix + "classification": []byte("public"),
			},
			wantErrCode: s3err.ErrNone,
			description: "Object with classification=public should be allowed",
		},
		{
			name: "confidential object - deny",
			objectEntry: map[string][]byte{
				s3_constants.AmzObjectTaggingPrefix + "classification": []byte("confidential"),
			},
			wantErrCode: s3err.ErrAccessDenied,
			description: "Object with classification=confidential should be denied by explicit deny",
		},
		{
			name:        "no classification tag - allow",
			objectEntry: map[string][]byte{},
			wantErrCode: s3err.ErrNone,
			description: "Object without classification tag should be allowed (deny condition not met)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/secure-bucket/test.txt", nil)

			errCode, checked := s3a.checkPolicyWithEntry(req, bucket, "/test.txt", string(s3_constants.ACTION_READ), "*", tt.objectEntry)

			assert.Equal(t, tt.wantErrCode, errCode, tt.description)
			assert.True(t, checked, "Policy should be evaluated")
		})
	}
}

// TestHasPolicyForBucket tests the HasPolicyForBucket method
func TestHasPolicyForBucket(t *testing.T) {
	engine := policy_engine.NewPolicyEngine()

	// Initially no policy
	assert.False(t, engine.HasPolicyForBucket("test-bucket"))

	// Set a policy
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*"
			}
		]
	}`
	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	require.NoError(t, err)

	// Now has policy
	assert.True(t, engine.HasPolicyForBucket("test-bucket"))

	// Other bucket still has no policy
	assert.False(t, engine.HasPolicyForBucket("other-bucket"))

	// Delete the policy
	err = engine.DeleteBucketPolicy("test-bucket")
	require.NoError(t, err)

	// No longer has policy
	assert.False(t, engine.HasPolicyForBucket("test-bucket"))
}
