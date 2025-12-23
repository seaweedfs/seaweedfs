package iam

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	input := "test"
	result := Hash(&input)
	assert.NotEmpty(t, result)
	assert.Len(t, result, 40) // SHA1 hex is 40 chars

	// Same input should produce same hash
	result2 := Hash(&input)
	assert.Equal(t, result, result2)

	// Different input should produce different hash
	different := "different"
	result3 := Hash(&different)
	assert.NotEqual(t, result, result3)
}

func TestGenerateRandomString(t *testing.T) {
	// Valid generation
	result, err := GenerateRandomString(10, CharsetUpper)
	assert.NoError(t, err)
	assert.Len(t, result, 10)

	// Different calls should produce different results (with high probability)
	result2, err := GenerateRandomString(10, CharsetUpper)
	assert.NoError(t, err)
	assert.NotEqual(t, result, result2)

	// Invalid length
	_, err = GenerateRandomString(0, CharsetUpper)
	assert.Error(t, err)

	_, err = GenerateRandomString(-1, CharsetUpper)
	assert.Error(t, err)

	// Empty charset
	_, err = GenerateRandomString(10, "")
	assert.Error(t, err)
}

func TestGenerateAccessKeyId(t *testing.T) {
	keyId, err := GenerateAccessKeyId()
	assert.NoError(t, err)
	assert.Len(t, keyId, AccessKeyIdLength)
}

func TestGenerateSecretAccessKey(t *testing.T) {
	secretKey, err := GenerateSecretAccessKey()
	assert.NoError(t, err)
	assert.Len(t, secretKey, SecretAccessKeyLength)
}

func TestStringSlicesEqual(t *testing.T) {
	tests := []struct {
		a        []string
		b        []string
		expected bool
	}{
		{[]string{"a", "b", "c"}, []string{"a", "b", "c"}, true},
		{[]string{"c", "b", "a"}, []string{"a", "b", "c"}, true}, // Order independent
		{[]string{"a", "b"}, []string{"a", "b", "c"}, false},
		{[]string{}, []string{}, true},
		{nil, nil, true},
		{[]string{"a"}, []string{"b"}, false},
	}

	for _, test := range tests {
		result := StringSlicesEqual(test.a, test.b)
		assert.Equal(t, test.expected, result)
	}
}

func TestMapToStatementAction(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{StatementActionAdmin, s3_constants.ACTION_ADMIN},
		{StatementActionWrite, s3_constants.ACTION_WRITE},
		{StatementActionRead, s3_constants.ACTION_READ},
		{StatementActionList, s3_constants.ACTION_LIST},
		{StatementActionDelete, s3_constants.ACTION_DELETE_BUCKET},
		{"unknown", ""},
	}

	for _, test := range tests {
		result := MapToStatementAction(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestMapToIdentitiesAction(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{s3_constants.ACTION_ADMIN, StatementActionAdmin},
		{s3_constants.ACTION_WRITE, StatementActionWrite},
		{s3_constants.ACTION_READ, StatementActionRead},
		{s3_constants.ACTION_LIST, StatementActionList},
		{s3_constants.ACTION_DELETE_BUCKET, StatementActionDelete},
		{"unknown", ""},
	}

	for _, test := range tests {
		result := MapToIdentitiesAction(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestMaskAccessKey(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"AKIAIOSFODNN7EXAMPLE", "AKIA***"},
		{"AKIA", "AKIA"},
		{"AKI", "AKI"},
		{"", ""},
	}

	for _, test := range tests {
		result := MaskAccessKey(test.input)
		assert.Equal(t, test.expected, result)
	}
}


