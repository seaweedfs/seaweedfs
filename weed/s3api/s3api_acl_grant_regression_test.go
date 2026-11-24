package s3api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

// These tests pin ACL behavior that already holds on master and that the
// grant permission validation must not change. They use only identifiers that
// exist on master, so they run, and pass, both before and after the change.

// TestValidateAndTransferGrantsKeepsGranteeErrors: every ACL master rejects for
// its grantees keeps the same error, also when its permission is invalid too,
// and also when an earlier grant in the list has an invalid permission.
func TestValidateAndTransferGrantsKeepsGranteeErrors(t *testing.T) {
	badGrantees := map[string]*s3.Grantee{
		"nil grantee":           nil,
		"nil type":              {ID: aws.String("alice")},
		"group without URI":     {Type: aws.String(s3_constants.GrantTypeGroup)},
		"unknown group URI":     {Type: aws.String(s3_constants.GrantTypeGroup), URI: aws.String("http://acs.amazonaws.com/groups/global/Nobody")},
		"canonical without ID":  {Type: aws.String(s3_constants.GrantTypeCanonicalUser)},
		"unknown account":       {Type: aws.String(s3_constants.GrantTypeCanonicalUser), ID: aws.String("mallory")},
		"email without address": {Type: aws.String(s3_constants.GrantTypeAmazonCustomerByEmail)},
		"unknown email":         {Type: aws.String(s3_constants.GrantTypeAmazonCustomerByEmail), EmailAddress: aws.String("nobody@example.com")},
		"unknown grantee type":  {Type: aws.String("Principal"), ID: aws.String("alice")},
	}
	permissions := map[string]*string{
		"valid permission":   aws.String("READ"),
		"invalid permission": aws.String("BOGUS"),
		"omitted permission": nil,
	}
	for granteeName, grantee := range badGrantees {
		for permissionName, permission := range permissions {
			t.Run(granteeName+"/"+permissionName, func(t *testing.T) {
				result, code := ValidateAndTransferGrants(grantPermissionAccounts{}, []*s3.Grant{
					{Grantee: grantee, Permission: permission},
				})
				require.Equal(t, s3err.ErrInvalidRequest, code)
				require.Nil(t, result)
			})
		}
		t.Run(granteeName+"/after an invalid permission", func(t *testing.T) {
			result, code := ValidateAndTransferGrants(grantPermissionAccounts{}, []*s3.Grant{
				{Grantee: validGrantees()["CanonicalUser"](), Permission: aws.String("BOGUS")},
				{Grantee: grantee, Permission: aws.String("READ")},
			})
			require.Equal(t, s3err.ErrInvalidRequest, code)
			require.Nil(t, result)
		})
	}
}

// TestValidateAndTransferGrantsKeepsValidListsUnchanged: a list of valid grants
// comes back in order, with email grantees turned into canonical users.
func TestValidateAndTransferGrantsKeepsValidListsUnchanged(t *testing.T) {
	grantees := validGrantees()
	result, code := ValidateAndTransferGrants(grantPermissionAccounts{}, []*s3.Grant{
		{Grantee: grantees["CanonicalUser"](), Permission: aws.String("FULL_CONTROL")},
		{Grantee: grantees["Group"](), Permission: aws.String("READ")},
		{Grantee: grantees["AmazonCustomerByEmail"](), Permission: aws.String("WRITE_ACP")},
	})
	require.Equal(t, s3err.ErrNone, code)
	require.Len(t, result, 3)
	require.Equal(t, "alice", aws.StringValue(result[0].Grantee.ID))
	require.Equal(t, "FULL_CONTROL", aws.StringValue(result[0].Permission))
	require.Equal(t, s3_constants.GranteeGroupAllUsers, aws.StringValue(result[1].Grantee.URI))
	require.Equal(t, "READ", aws.StringValue(result[1].Permission))
	require.Equal(t, s3_constants.GrantTypeCanonicalUser, aws.StringValue(result[2].Grantee.Type))
	require.Equal(t, "alice", aws.StringValue(result[2].Grantee.ID))
	require.Nil(t, result[2].Grantee.EmailAddress)
	require.Equal(t, "WRITE_ACP", aws.StringValue(result[2].Permission))
}

// TestExtractAclKeepsRequestLevelErrors: errors decided before any grant is
// looked at stay as they are, whatever the grants' permissions say.
func TestExtractAclKeepsRequestLevelErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
		want s3err.ErrorCode
	}{
		{name: "not XML", body: "not xml", want: s3err.ErrInvalidRequest},
		{name: "no owner", body: `<AccessControlPolicy><AccessControlList></AccessControlList></AccessControlPolicy>`, want: s3err.ErrInvalidRequest},
		{name: "owner without ID", body: `<AccessControlPolicy><Owner></Owner></AccessControlPolicy>`, want: s3err.ErrInvalidRequest},
		{name: "other owner, valid grant", body: strings.Replace(aclRequestBody("<Permission>READ</Permission>"), "owner-id", "someone-else", 1), want: s3err.ErrAccessDenied},
		{name: "other owner, invalid grant", body: strings.Replace(aclRequestBody("<Permission>BOGUS</Permission>"), "owner-id", "someone-else", 1), want: s3err.ErrAccessDenied},
		{name: "other owner, omitted permission", body: strings.Replace(aclRequestBody(""), "owner-id", "someone-else", 1), want: s3err.ErrAccessDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			grants, code := extractAclFromBody(tc.body)
			require.Equal(t, tc.want, code)
			require.Nil(t, grants)
		})
	}
}

// TestExtractAclKeepsEmptyAndHeaderPaths: an empty ACL list is still accepted,
// and a request without a body still takes its ACL from the canned header or
// defaults to the owner's FULL_CONTROL.
func TestExtractAclKeepsEmptyAndHeaderPaths(t *testing.T) {
	t.Run("empty AccessControlList", func(t *testing.T) {
		grants, code := extractAclFromBody(aclRequestBody())
		require.Equal(t, s3err.ErrNone, code)
		require.Empty(t, grants)
	})
	headerRequest := func(cannedAcl string) ([]*s3.Grant, s3err.ErrorCode) {
		r := httptest.NewRequest(http.MethodPut, "/bucket/object?acl", http.NoBody)
		if cannedAcl != "" {
			r.Header.Set(s3_constants.AmzCannedAcl, cannedAcl)
		}
		return ExtractAcl(r, grantPermissionAccounts{}, "", "owner-id", "owner-id", "owner-id")
	}
	t.Run("no body, no header", func(t *testing.T) {
		grants, code := headerRequest("")
		require.Equal(t, s3err.ErrNone, code)
		require.Len(t, grants, 1)
		require.Equal(t, "owner-id", aws.StringValue(grants[0].Grantee.ID))
		require.Equal(t, "FULL_CONTROL", aws.StringValue(grants[0].Permission))
	})
	t.Run("canned public-read", func(t *testing.T) {
		grants, code := headerRequest(s3_constants.CannedAclPublicRead)
		require.Equal(t, s3err.ErrNone, code)
		require.Len(t, grants, 2)
		require.Equal(t, "FULL_CONTROL", aws.StringValue(grants[0].Permission))
		require.Equal(t, s3_constants.GranteeGroupAllUsers, aws.StringValue(grants[1].Grantee.URI))
		require.Equal(t, "READ", aws.StringValue(grants[1].Permission))
	})
	t.Run("unknown canned ACL", func(t *testing.T) {
		_, code := headerRequest("everyone")
		require.Equal(t, s3err.ErrInvalidRequest, code)
	})
}

// TestParseAndValidateAclHeadersKeepsCustomGrants: the x-amz-grant-* headers of
// PutObject and CreateBucket also go through ValidateAndTransferGrants; each
// header still yields its own permission and the request is still accepted.
func TestParseAndValidateAclHeadersKeepsCustomGrants(t *testing.T) {
	headers := map[string]string{
		s3_constants.AmzAclFullControl: "FULL_CONTROL",
		s3_constants.AmzAclRead:        "READ",
		s3_constants.AmzAclWrite:       "WRITE",
		s3_constants.AmzAclReadAcp:     "READ_ACP",
		s3_constants.AmzAclWriteAcp:    "WRITE_ACP",
	}
	for header, permission := range headers {
		t.Run(header, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "/bucket/object", http.NoBody)
			r.Header.Set(header, `id="alice", emailAddress="alice@example.com", uri="`+s3_constants.GranteeGroupAllUsers+`"`)
			ownerId, grants, code := ParseAndValidateAclHeaders(r, grantPermissionAccounts{}, "", "owner-id", "owner-id", false)
			require.Equal(t, s3err.ErrNone, code)
			require.Equal(t, "owner-id", ownerId)
			require.Len(t, grants, 3)
			for _, grant := range grants {
				require.Equal(t, permission, aws.StringValue(grant.Permission))
			}
		})
	}
}

// TestBuildAccessControlListKeepsStoredGrants: the GetObjectAcl / GetBucketAcl
// response for stored grants that carry a permission is the same as before,
// including a stored permission string the write path would now refuse.
func TestBuildAccessControlListKeepsStoredGrants(t *testing.T) {
	t.Run("no stored grants", func(t *testing.T) {
		acl := buildAccessControlList(grantPermissionAccounts{}, nil, "owner-id", "Owner")
		require.Len(t, acl.Grant, 1)
		require.Equal(t, "owner-id", acl.Grant[0].Grantee.ID)
		require.Equal(t, "Owner", acl.Grant[0].Grantee.DisplayName)
		require.Equal(t, Permission("FULL_CONTROL"), acl.Grant[0].Permission)
	})
	t.Run("stored grants", func(t *testing.T) {
		stored, err := json.Marshal([]*s3.Grant{
			{Grantee: &s3.Grantee{Type: aws.String(s3_constants.GrantTypeCanonicalUser), ID: aws.String("alice")}, Permission: aws.String("WRITE")},
			{Grantee: &s3.Grantee{Type: aws.String(s3_constants.GrantTypeGroup), URI: aws.String(s3_constants.GranteeGroupAllUsers)}, Permission: aws.String("READ")},
			{Grantee: &s3.Grantee{Type: aws.String(s3_constants.GrantTypeCanonicalUser), ID: aws.String("owner-id")}, Permission: aws.String("BOGUS")},
		})
		require.NoError(t, err)
		acl := buildAccessControlList(grantPermissionAccounts{}, GetAcpGrants(map[string][]byte{s3_constants.ExtAmzAclKey: stored}), "owner-id", "Owner")
		require.Len(t, acl.Grant, 3)
		require.Equal(t, "alice", acl.Grant[0].Grantee.ID)
		require.Equal(t, "Alice", acl.Grant[0].Grantee.DisplayName)
		require.Equal(t, s3_constants.GrantTypeCanonicalUser, acl.Grant[0].Grantee.XMLXSI)
		require.Equal(t, Permission("WRITE"), acl.Grant[0].Permission)
		require.Equal(t, s3_constants.GranteeGroupAllUsers, acl.Grant[1].Grantee.URI)
		require.Equal(t, Permission("READ"), acl.Grant[1].Permission)
		require.Equal(t, Permission("BOGUS"), acl.Grant[2].Permission)
	})
}

// TestValidateAndTransferGrantsAcceptsDocumentedPermissions: every documented
// value is accepted for every grantee type and kept unchanged, including when
// an email grantee is transferred to a canonical-user grant.
func TestValidateAndTransferGrantsAcceptsDocumentedPermissions(t *testing.T) {
	for granteeName, grantee := range validGrantees() {
		for _, permission := range documentedGrantPermissions {
			t.Run(granteeName+"/"+permission, func(t *testing.T) {
				result, code := ValidateAndTransferGrants(grantPermissionAccounts{}, []*s3.Grant{
					{Grantee: grantee(), Permission: aws.String(permission)},
				})
				require.Equal(t, s3err.ErrNone, code)
				require.Len(t, result, 1)
				require.Equal(t, permission, aws.StringValue(result[0].Permission))
			})
		}
	}
}

// TestExtractAclBodyAcceptsDocumentedPermissions is the request-level form of
// the accept test: the XML body PutObjectAcl and PutBucketAcl parse.
func TestExtractAclBodyAcceptsDocumentedPermissions(t *testing.T) {
	for _, permission := range documentedGrantPermissions {
		t.Run(permission, func(t *testing.T) {
			grants, code := extractAclFromBody(aclRequestBody("<Permission>" + permission + "</Permission>"))
			require.Equal(t, s3err.ErrNone, code)
			require.Len(t, grants, 1)
			require.Equal(t, permission, aws.StringValue(grants[0].Permission))
		})
	}
}
