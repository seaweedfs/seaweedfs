package s3api

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

// TestParseCustomAclHeaderList covers the wire syntax separately from account
// resolution, including quoted delimiters and rejection without partial grants.
func TestParseCustomAclHeaderList(t *testing.T) {
	tests := []struct {
		name, input string
		values      []string
		invalid     bool
	}{
		{name: "absent"},
		{name: "single", input: `id="alice"`, values: []string{"alice"}},
		{name: "comma without space", input: `id="alice",id="bob"`, values: []string{"alice", "bob"}},
		{name: "comma with space", input: `id="alice", id="bob"`, values: []string{"alice", "bob"}},
		{name: "optional whitespace", input: " id = \"alice\" ,\t id=\"bob\" ", values: []string{"alice", "bob"}},
		{name: "quoted comma and equals", input: `id="a,b=c",id="bob"`, values: []string{"a,b=c", "bob"}},
		{name: "escaped quote", input: `id="a\"b",id="bob"`, values: []string{`a"b`, "bob"}},
		{name: "email", input: `emailAddress="a=b@example.com"`, values: []string{"a=b@example.com"}},
		{name: "group", input: `uri="http://acs.amazonaws.com/groups/global/AllUsers"`, values: []string{s3_constants.GranteeGroupAllUsers}},
		{name: "unknown type", input: `account="alice"`, invalid: true},
		{name: "mixed unknown type", input: `id="alice",principal="bob"`, invalid: true},
		{name: "empty grantee", input: `id=""`, invalid: true},
		{name: "unquoted", input: `id=alice`, invalid: true},
		{name: "unterminated", input: `id="alice`, invalid: true},
		{name: "trailing comma", input: `id="alice",`, invalid: true},
		{name: "empty element", input: `id="alice",,id="bob"`, invalid: true},
		{name: "missing comma", input: `id="alice" id="bob"`, invalid: true},
		{name: "invalid escape", input: `id="a\q"`, invalid: true},
		{name: "whitespace only", input: "  ", invalid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &s3.Grant{Permission: aws.String(s3_constants.PermissionFullControl)}
			grants := []*s3.Grant{original}
			code := ParseCustomAclHeader(tt.input, s3_constants.PermissionRead, &grants)
			if tt.invalid {
				require.Equal(t, s3err.ErrInvalidRequest, code)
				require.Equal(t, []*s3.Grant{original}, grants, "invalid lists must not leave partial grants")
				return
			}
			require.Equal(t, s3err.ErrNone, code)
			require.Len(t, grants, 1+len(tt.values))
			for i, value := range tt.values {
				grant := grants[i+1]
				actual := aws.StringValue(grant.Grantee.ID) + aws.StringValue(grant.Grantee.EmailAddress) + aws.StringValue(grant.Grantee.URI)
				require.Equal(t, value, actual)
				require.Equal(t, s3_constants.PermissionRead, aws.StringValue(grant.Permission))
			}
		})
	}
}
