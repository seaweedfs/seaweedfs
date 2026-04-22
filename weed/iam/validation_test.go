package iam

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
)

func TestValidateCallerSuppliedAccessKeyId(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"empty", "", true},
		{"three chars", "abc", true},
		{"four chars ok", "abcd", false},
		{"mixed case alnum", "MyAppKey123", false},
		{"128 chars ok", strings.Repeat("a", 128), false},
		{"129 chars too long", strings.Repeat("a", 129), true},
		{"slash rejected", "foo/bar", true},
		{"equals rejected", "foo=bar", true},
		{"dash rejected", "foo-bar", true},
		{"underscore rejected", "foo_bar", true},
		{"unicode rejected", "fooö123", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCallerSuppliedAccessKeyId(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCallerSuppliedSecretAccessKey(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"empty", "", true},
		{"seven chars", "abcdefg", true},
		{"eight chars ok", "abcdefgh", false},
		{"128 chars ok", strings.Repeat("a", 128), false},
		{"129 chars too long", strings.Repeat("a", 129), true},
		{"non-alnum allowed in secret", "sec/ret=1", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCallerSuppliedSecretAccessKey(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFindAccessKeyOwner(t *testing.T) {
	s3cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "alice",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "AKIAALICE1", SecretKey: "s"},
				},
			},
			{Name: "bob"},
		},
		ServiceAccounts: []*iam_pb.ServiceAccount{
			{Id: "svc-1", Credential: &iam_pb.Credential{AccessKey: "SVCKEY1"}},
			{Id: "svc-2"},
		},
	}

	t.Run("matches user", func(t *testing.T) {
		owner := FindAccessKeyOwner(s3cfg, "AKIAALICE1")
		assert.NotNil(t, owner)
		assert.Equal(t, "user", owner.Type)
		assert.Equal(t, "alice", owner.Name)
	})
	t.Run("matches service account", func(t *testing.T) {
		owner := FindAccessKeyOwner(s3cfg, "SVCKEY1")
		assert.NotNil(t, owner)
		assert.Equal(t, "service account", owner.Type)
		assert.Equal(t, "svc-1", owner.Name)
	})
	t.Run("no match", func(t *testing.T) {
		assert.Nil(t, FindAccessKeyOwner(s3cfg, "NOTTAKEN"))
	})
	t.Run("empty key", func(t *testing.T) {
		assert.Nil(t, FindAccessKeyOwner(s3cfg, ""))
	})
	t.Run("nil config", func(t *testing.T) {
		assert.Nil(t, FindAccessKeyOwner(nil, "anything"))
	})
}
