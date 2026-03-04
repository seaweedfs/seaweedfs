package iamapi

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/service/iam"
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

func TestMultipleInlinePoliciesAggregateActions(t *testing.T) {
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	}

	policy1JSON := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::bucket-a/*"]
    }
  ]
}`

	policy2JSON := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": ["arn:aws:s3:::bucket-b/*"]
    }
  ]
}`

	iama := &IamApiServer{
		s3ApiConfig: &mockIamS3ApiConfig{},
	}

	// Put first inline policy
	putValues1 := url.Values{
		"UserName":       []string{"alice"},
		"PolicyName":     []string{"policy-read"},
		"PolicyDocument": []string{policy1JSON},
	}
	_, iamErr := iama.PutUserPolicy(s3cfg, putValues1)
	assert.Nil(t, iamErr)

	// Check that alice's actions include read operations
	aliceIdent := s3cfg.Identities[0]
	assert.Greater(t, len(aliceIdent.Actions), 0, "Actions should not be empty after first policy")

	// Put second inline policy
	putValues2 := url.Values{
		"UserName":       []string{"alice"},
		"PolicyName":     []string{"policy-write"},
		"PolicyDocument": []string{policy2JSON},
	}
	_, iamErr = iama.PutUserPolicy(s3cfg, putValues2)
	assert.Nil(t, iamErr)

	// Check that alice now has aggregated actions from both policies
	// Should include Read and List (from policy1) and Write (from policy2)
	// with resource paths indicating which policy they came from

	// Build a set of actual action strings for exact membership checks
	actionSet := make(map[string]bool)
	for _, action := range aliceIdent.Actions {
		actionSet[action] = true
	}

	// Expected actions from both policies:
	// - policy1: GetObject, ListBucket on bucket-a/*  → "Read:bucket-a/*", "List:bucket-a/*"
	// - policy2: PutObject on bucket-b/*  → "Write:bucket-b/*"
	expectedActions := []string{
		"Read:bucket-a/*",
		"List:bucket-a/*",
		"Write:bucket-b/*",
	}

	for _, expectedAction := range expectedActions {
		assert.True(t, actionSet[expectedAction], "Expected action '%s' not found in aggregated actions. Got: %v", expectedAction, aliceIdent.Actions)
	}
}

// Helper to create a mock IamApiServer with pre-populated managed policies
func newTestIamApiServer(policies Policies) *IamApiServer {
	return &IamApiServer{
		s3ApiConfig: &mockIamS3ApiConfig{policies: policies},
	}
}

func TestGetPolicy(t *testing.T) {
	policyDoc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::*"),
			},
		},
	}
	iama := newTestIamApiServer(Policies{
		Policies: map[string]policy_engine.PolicyDocument{"my-policy": policyDoc},
	})
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Success case
	values := url.Values{"PolicyArn": []string{"arn:aws:iam:::policy/my-policy"}}
	resp, iamErr := iama.GetPolicy(s3cfg, values)
	assert.Nil(t, iamErr)
	assert.Equal(t, "my-policy", *resp.GetPolicyResult.Policy.PolicyName)
	assert.Equal(t, "arn:aws:iam:::policy/my-policy", *resp.GetPolicyResult.Policy.Arn)
	policyName := "my-policy"
	expectedId := Hash(&policyName)
	assert.Equal(t, expectedId, *resp.GetPolicyResult.Policy.PolicyId)

	// Not found case
	values = url.Values{"PolicyArn": []string{"arn:aws:iam:::policy/nonexistent"}}
	_, iamErr = iama.GetPolicy(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeNoSuchEntityException, iamErr.Code)

	// Invalid ARN
	values = url.Values{"PolicyArn": []string{"invalid-arn"}}
	_, iamErr = iama.GetPolicy(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeInvalidInputException, iamErr.Code)

	// Empty ARN
	values = url.Values{"PolicyArn": []string{""}}
	_, iamErr = iama.GetPolicy(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeInvalidInputException, iamErr.Code)
}

func TestDeletePolicy(t *testing.T) {
	policyDoc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::*"),
			},
		},
	}
	mock := &mockIamS3ApiConfig{policies: Policies{
		Policies: map[string]policy_engine.PolicyDocument{"my-policy": policyDoc},
	}}
	iama := &IamApiServer{s3ApiConfig: mock}

	// Reject deletion when policy is attached to a user (AWS-compatible behavior)
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "alice",
			PolicyNames: []string{"my-policy"},
		}},
	}
	values := url.Values{"PolicyArn": []string{"arn:aws:iam:::policy/my-policy"}}
	_, iamErr := iama.DeletePolicy(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeDeleteConflictException, iamErr.Code)

	// Succeed when no users are attached
	s3cfgEmpty := &iam_pb.S3ApiConfiguration{}
	_, iamErr = iama.DeletePolicy(s3cfgEmpty, values)
	assert.Nil(t, iamErr)

	// Verify deleted
	_, iamErr = iama.GetPolicy(s3cfgEmpty, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeNoSuchEntityException, iamErr.Code)
}

func TestListPolicies(t *testing.T) {
	iama := newTestIamApiServer(Policies{})
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Empty case
	resp, iamErr := iama.ListPolicies(s3cfg, url.Values{})
	assert.Nil(t, iamErr)
	assert.Empty(t, resp.ListPoliciesResult.Policies)

	// Populated case
	policyDoc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::*"),
			},
		},
	}
	iama = newTestIamApiServer(Policies{
		Policies: map[string]policy_engine.PolicyDocument{
			"policy-a": policyDoc,
			"policy-b": policyDoc,
		},
	})

	resp, iamErr = iama.ListPolicies(s3cfg, url.Values{})
	assert.Nil(t, iamErr)
	assert.Equal(t, 2, len(resp.ListPoliciesResult.Policies))
	for _, p := range resp.ListPoliciesResult.Policies {
		name := *p.PolicyName
		expectedId := Hash(&name)
		assert.Equal(t, expectedId, *p.PolicyId, "PolicyId should be Hash(policyName) for %s", name)
	}
}

func TestAttachUserPolicy(t *testing.T) {
	policyDoc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::*"),
			},
		},
	}
	iama := newTestIamApiServer(Policies{
		Policies: map[string]policy_engine.PolicyDocument{"my-policy": policyDoc},
	})
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	}

	// Success case
	values := url.Values{
		"UserName":  []string{"alice"},
		"PolicyArn": []string{"arn:aws:iam:::policy/my-policy"},
	}
	_, iamErr := iama.AttachUserPolicy(s3cfg, values)
	assert.Nil(t, iamErr)
	assert.Contains(t, s3cfg.Identities[0].PolicyNames, "my-policy")
	// Verify actions were computed from the managed policy
	assert.Greater(t, len(s3cfg.Identities[0].Actions), 0)

	// Idempotent re-attach
	_, iamErr = iama.AttachUserPolicy(s3cfg, values)
	assert.Nil(t, iamErr)
	// Should still have exactly one entry
	count := 0
	for _, name := range s3cfg.Identities[0].PolicyNames {
		if name == "my-policy" {
			count++
		}
	}
	assert.Equal(t, 1, count)

	// Policy not found
	values = url.Values{
		"UserName":  []string{"alice"},
		"PolicyArn": []string{"arn:aws:iam:::policy/nonexistent"},
	}
	_, iamErr = iama.AttachUserPolicy(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeNoSuchEntityException, iamErr.Code)

	// User not found
	values = url.Values{
		"UserName":  []string{"bob"},
		"PolicyArn": []string{"arn:aws:iam:::policy/my-policy"},
	}
	_, iamErr = iama.AttachUserPolicy(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeNoSuchEntityException, iamErr.Code)
}

func TestManagedPolicyActionsPreservedAcrossInlineMutations(t *testing.T) {
	managedPolicyDoc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::*"),
			},
		},
	}
	iama := newTestIamApiServer(Policies{
		Policies: map[string]policy_engine.PolicyDocument{"my-policy": managedPolicyDoc},
	})
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	}

	// Attach managed policy
	_, iamErr := iama.AttachUserPolicy(s3cfg, url.Values{
		"UserName":  []string{"alice"},
		"PolicyArn": []string{"arn:aws:iam:::policy/my-policy"},
	})
	assert.Nil(t, iamErr)
	assert.Contains(t, s3cfg.Identities[0].Actions, "Read", "Managed policy should grant Read action")

	// Add an inline policy
	inlinePolicyJSON := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket-x/*"}]}`
	_, iamErr = iama.PutUserPolicy(s3cfg, url.Values{
		"UserName":       []string{"alice"},
		"PolicyName":     []string{"inline-write"},
		"PolicyDocument": []string{inlinePolicyJSON},
	})
	assert.Nil(t, iamErr)

	// Should have both managed (Read) and inline (Write:bucket-x/*) actions
	actionSet := make(map[string]bool)
	for _, a := range s3cfg.Identities[0].Actions {
		actionSet[a] = true
	}
	assert.True(t, actionSet["Read"], "Managed policy Read action should persist after PutUserPolicy")
	assert.True(t, actionSet["Write:bucket-x/*"], "Inline policy Write action should be present")

	// Delete the inline policy
	_, iamErr = iama.DeleteUserPolicy(s3cfg, url.Values{
		"UserName":   []string{"alice"},
		"PolicyName": []string{"inline-write"},
	})
	assert.Nil(t, iamErr)

	// Managed policy actions should still be present
	assert.Contains(t, s3cfg.Identities[0].PolicyNames, "my-policy", "Managed policy should still be attached")
	assert.Contains(t, s3cfg.Identities[0].Actions, "Read", "Managed policy Read action should persist after DeleteUserPolicy")
}

func TestDetachUserPolicy(t *testing.T) {
	policyDoc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::*"),
			},
		},
	}
	iama := newTestIamApiServer(Policies{
		Policies: map[string]policy_engine.PolicyDocument{"my-policy": policyDoc},
	})
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice", PolicyNames: []string{"my-policy"}}},
	}

	values := url.Values{
		"UserName":  []string{"alice"},
		"PolicyArn": []string{"arn:aws:iam:::policy/my-policy"},
	}
	_, iamErr := iama.DetachUserPolicy(s3cfg, values)
	assert.Nil(t, iamErr)
	assert.Empty(t, s3cfg.Identities[0].PolicyNames)

	// Detach again should fail (not attached)
	_, iamErr = iama.DetachUserPolicy(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeNoSuchEntityException, iamErr.Code)
}

func TestListAttachedUserPolicies(t *testing.T) {
	iama := newTestIamApiServer(Policies{})
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice", PolicyNames: []string{"policy-a", "policy-b"}}},
	}

	values := url.Values{"UserName": []string{"alice"}}
	resp, iamErr := iama.ListAttachedUserPolicies(s3cfg, values)
	assert.Nil(t, iamErr)
	assert.Equal(t, 2, len(resp.ListAttachedUserPoliciesResult.AttachedPolicies))
	assert.Equal(t, "policy-a", *resp.ListAttachedUserPoliciesResult.AttachedPolicies[0].PolicyName)
	assert.Equal(t, "arn:aws:iam:::policy/policy-a", *resp.ListAttachedUserPoliciesResult.AttachedPolicies[0].PolicyArn)

	// User not found
	values = url.Values{"UserName": []string{"bob"}}
	_, iamErr = iama.ListAttachedUserPolicies(s3cfg, values)
	assert.NotNil(t, iamErr)
	assert.Equal(t, iam.ErrCodeNoSuchEntityException, iamErr.Code)
}
