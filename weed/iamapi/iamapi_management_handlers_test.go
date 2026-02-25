package iamapi

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/stretchr/testify/assert"
)

// mockIamS3ApiConfig is a mock for testing
type mockIamS3ApiConfig struct {
	policies Policies
}

func (m *mockIamS3ApiConfig) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return nil
}

func (m *mockIamS3ApiConfig) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return nil
}

func (m *mockIamS3ApiConfig) GetPolicies(policies *Policies) (err error) {
	*policies = m.policies
	if m.policies.Policies == nil {
		return filer_pb.ErrNotFound
	}
	return nil
}

func (m *mockIamS3ApiConfig) PutPolicies(policies *Policies) (err error) {
	m.policies = *policies
	return nil
}

func TestGetActionsUserPath(t *testing.T) {

	policyDocument := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:Put*", "s3:PutBucketAcl", "s3:Get*", "s3:GetBucketAcl", "s3:List*", "s3:Tagging*", "s3:DeleteBucket*"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::shared/user-Alice/*"),
			},
		},
	}

	actions, _ := GetActions(&policyDocument)

	expectedActions := []string{
		"Write:shared/user-Alice/*",
		"WriteAcp:shared/user-Alice/*",
		"Read:shared/user-Alice/*",
		"ReadAcp:shared/user-Alice/*",
		"List:shared/user-Alice/*",
		"Tagging:shared/user-Alice/*",
		"DeleteBucket:shared/user-Alice/*",
	}
	assert.Equal(t, expectedActions, actions)
}

func TestGetActionsWildcardPath(t *testing.T) {

	policyDocument := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:Get*", "s3:PutBucketAcl"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::*"),
			},
		},
	}

	actions, _ := GetActions(&policyDocument)

	expectedActions := []string{
		"Read",
		"WriteAcp",
	}
	assert.Equal(t, expectedActions, actions)
}

func TestGetActionsInvalidAction(t *testing.T) {
	policyDocument := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:InvalidAction"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::shared/user-Alice/*"),
			},
		},
	}

	_, err := GetActions(&policyDocument)
	assert.NotNil(t, err)
	assert.Equal(t, "not a valid action: 'InvalidAction'", err.Error())
}

func TestPutGetUserPolicyPreservesStatements(t *testing.T) {
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	}
	policyJSON := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/*",
        "arn:aws:s3:::test/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/*",
        "arn:aws:s3:::test/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/*",
        "arn:aws:s3:::test/*"
      ]
    }
  ]
}`

	iama := &IamApiServer{
		s3ApiConfig: &mockIamS3ApiConfig{},
	}
	putValues := url.Values{
		"UserName":       []string{"alice"},
		"PolicyName":     []string{"inline-policy"},
		"PolicyDocument": []string{policyJSON},
	}
	_, iamErr := iama.PutUserPolicy(s3cfg, putValues)
	assert.Nil(t, iamErr)

	getValues := url.Values{
		"UserName":   []string{"alice"},
		"PolicyName": []string{"inline-policy"},
	}
	resp, iamErr := iama.GetUserPolicy(s3cfg, getValues)
	assert.Nil(t, iamErr)

	// Verify that key policy properties are preserved (not merged or lost)
	var got policy_engine.PolicyDocument
	assert.NoError(t, json.Unmarshal([]byte(resp.GetUserPolicyResult.PolicyDocument), &got))

	// Assert we have exactly 3 statements (not merged into 1 or lost)
	assert.Equal(t, 3, len(got.Statement))

	// Assert that DeleteObject statement is present (was lost in the bug)
	deleteObjectFound := false
	for _, stmt := range got.Statement {
		if len(stmt.Action.Strings()) > 0 {
			for _, action := range stmt.Action.Strings() {
				if action == "s3:DeleteObject" {
					deleteObjectFound = true
					break
				}
			}
		}
	}
	assert.True(t, deleteObjectFound, "s3:DeleteObject action was lost")
}
