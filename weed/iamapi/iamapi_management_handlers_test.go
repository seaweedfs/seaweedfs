package iamapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetActionsUserPath(t *testing.T) {

	policyDocument := PolicyDocument{
		Version: "2012-10-17",
		Statement: []*Statement{
			{
				Effect: "Allow",
				Action: []string{
					"s3:Put*",
					"s3:PutBucketAcl",
					"s3:Get*",
					"s3:GetBucketAcl",
					"s3:List*",
					"s3:Tagging*",
					"s3:DeleteBucket*",
				},
				Resource: []string{
					"arn:aws:s3:::shared/user-Alice/*",
				},
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

	policyDocument := PolicyDocument{
		Version: "2012-10-17",
		Statement: []*Statement{
			{
				Effect: "Allow",
				Action: []string{
					"s3:Get*",
					"s3:PutBucketAcl",
				},
				Resource: []string{
					"arn:aws:s3:::*",
				},
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
	policyDocument := PolicyDocument{
		Version: "2012-10-17",
		Statement: []*Statement{
			{
				Effect: "Allow",
				Action: []string{
					"s3:InvalidAction",
				},
				Resource: []string{
					"arn:aws:s3:::shared/user-Alice/*",
				},
			},
		},
	}

	_, err := GetActions(&policyDocument)
	assert.NotNil(t, err)
	assert.Equal(t, "not a valid action: 'InvalidAction'", err.Error())
}
