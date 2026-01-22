package iamapi

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy_engine"
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
