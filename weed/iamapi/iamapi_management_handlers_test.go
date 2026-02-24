package iamapi

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/stretchr/testify/assert"
)

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
	policyDocuments = map[string]*policy_engine.PolicyDocument{}

	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	}
	policyJSON := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3:::my-bucket/*","arn:aws:s3:::test/*"]},{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::my-bucket/*","arn:aws:s3:::test/*"]},{"Effect":"Allow","Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::my-bucket/*","arn:aws:s3:::test/*"]}]}`

	iama := &IamApiServer{}
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

	var got policy_engine.PolicyDocument
	assert.NoError(t, json.Unmarshal([]byte(resp.GetUserPolicyResult.PolicyDocument), &got))

	var want policy_engine.PolicyDocument
	assert.NoError(t, json.Unmarshal([]byte(policyJSON), &want))
	assert.Equal(t, want, got)
}
