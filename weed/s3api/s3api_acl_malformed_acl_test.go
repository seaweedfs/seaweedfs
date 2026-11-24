package s3api

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

// TestInvalidGrantPermissionIsMalformedACL: an ACL whose grantees are all valid
// but whose grant permission is omitted or unknown is rejected with S3's
// MalformedACLError, the error S3 returns for an AccessControlPolicy that does
// not validate against its schema.
func TestInvalidGrantPermissionIsMalformedACL(t *testing.T) {
	for granteeName, grantee := range validGrantees() {
		for _, permission := range invalidGrantPermissions {
			t.Run(granteeName+"/"+describePermission(permission), func(t *testing.T) {
				result, code := ValidateAndTransferGrants(grantPermissionAccounts{}, []*s3.Grant{
					{Grantee: grantee(), Permission: permission},
				})
				require.Equal(t, s3err.ErrMalformedACL, code)
				require.Nil(t, result)
			})
		}
	}
}

// TestExtractAclBodyWithInvalidPermissionIsMalformedACL is the request-level
// form: PutObjectAcl and PutBucketAcl answer such a body with MalformedACLError.
func TestExtractAclBodyWithInvalidPermissionIsMalformedACL(t *testing.T) {
	bodies := map[string]string{
		"omitted Permission element": aclRequestBody(""),
		"empty Permission element":   aclRequestBody("<Permission></Permission>"),
		"lowercase value":            aclRequestBody("<Permission>read</Permission>"),
		"unknown value":              aclRequestBody("<Permission>READ_WRITE</Permission>"),
		"valid then unknown":         aclRequestBody("<Permission>READ</Permission>", "<Permission>ALL</Permission>"),
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			grants, code := extractAclFromBody(body)
			require.Equal(t, s3err.ErrMalformedACL, code)
			require.Nil(t, grants)
		})
	}
}
