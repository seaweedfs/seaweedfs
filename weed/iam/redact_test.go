package iam

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedactSensitiveFormValues(t *testing.T) {
	in := url.Values{
		"Action":          []string{"CreateAccessKey"},
		"UserName":        []string{"alice"},
		"AccessKeyId":     []string{"AKIAALICE1"},
		"SecretAccessKey": []string{"very-secret"},
		"Password":        []string{"hunter2"},
		"NewPassword":     []string{"new-hunter2"},
		"OldPassword":     []string{"old-hunter2"},
		"PrivateKey":      []string{"-----BEGIN-----"},
		"SessionToken":    []string{"tok"},
	}
	out := RedactSensitiveFormValues(in)

	assert.Equal(t, []string{"CreateAccessKey"}, out["Action"])
	assert.Equal(t, []string{"alice"}, out["UserName"])
	assert.Equal(t, []string{"AKIAALICE1"}, out["AccessKeyId"])
	for _, k := range []string{"SecretAccessKey", "Password", "NewPassword", "OldPassword", "PrivateKey", "SessionToken"} {
		assert.Equal(t, []string{"[REDACTED]"}, out[k], "key %q should be redacted", k)
	}

	// Input is not mutated.
	assert.Equal(t, []string{"very-secret"}, in["SecretAccessKey"])
}
