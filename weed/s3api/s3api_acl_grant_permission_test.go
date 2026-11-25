package s3api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

// The S3 Grant data type (API_Grant) has two members, Grantee and Permission,
// and both are optional ("Required: No"): a client may leave either one out.
// Permission, when present, is one of the documented values
// FULL_CONTROL | WRITE | WRITE_ACP | READ | READ_ACP, compared exactly.
// The SDK model generated from the same API definition agrees: s3.Grant has no
// `required` tag on Permission, and s3.Permission_Values() lists the five values.
//
// Because the member is optional, the server must check that a client-supplied
// grant carries a permission and that the permission is one of these values,
// instead of assuming a well-formed one is always there. A grant without a
// permission grants nothing, so the ACL must be rejected as a client error.
var documentedGrantPermissions = []string{"FULL_CONTROL", "WRITE", "WRITE_ACP", "READ", "READ_ACP"}

// invalidGrantPermissions are values a client can send that are not one of the
// documented permissions. nil stands for an omitted Permission member.
var invalidGrantPermissions = []*string{
	nil,
	aws.String(""),
	aws.String("read"),
	aws.String("Read"),
	aws.String("full_control"),
	aws.String("FULLCONTROL"),
	aws.String("FULL CONTROL"),
	aws.String("READ "),
	aws.String(" READ"),
	aws.String("READ,WRITE"),
	aws.String("READ_WRITE"),
	aws.String("WRITE_ACL"),
	aws.String("ALL"),
}

func describePermission(permission *string) string {
	if permission == nil {
		return "omitted"
	}
	return fmt.Sprintf("%q", *permission)
}

// grantPermissionAccounts resolves the accounts the tests grant to, so a
// rejection can only come from the permission, never from the grantee.
type grantPermissionAccounts struct{}

func (grantPermissionAccounts) GetAccountNameById(canonicalId string) string {
	switch canonicalId {
	case "owner-id":
		return "Owner"
	case "alice":
		return "Alice"
	}
	return ""
}

func (grantPermissionAccounts) GetAccountIdByEmail(email string) string {
	if email == "alice@example.com" {
		return "alice"
	}
	return ""
}

func (grantPermissionAccounts) GetAccountIdByIdentityName(string) string { return "" }

// validGrantees covers every grantee type ValidateAndTransferGrants accepts,
// each with a grantee that passes its own checks.
func validGrantees() map[string]func() *s3.Grantee {
	return map[string]func() *s3.Grantee{
		"CanonicalUser": func() *s3.Grantee {
			return &s3.Grantee{Type: aws.String(s3_constants.GrantTypeCanonicalUser), ID: aws.String("alice")}
		},
		"Group": func() *s3.Grantee {
			return &s3.Grantee{Type: aws.String(s3_constants.GrantTypeGroup), URI: aws.String(s3_constants.GranteeGroupAllUsers)}
		},
		"AmazonCustomerByEmail": func() *s3.Grantee {
			return &s3.Grantee{Type: aws.String(s3_constants.GrantTypeAmazonCustomerByEmail), EmailAddress: aws.String("alice@example.com")}
		},
	}
}

func requireClientError(t *testing.T, code s3err.ErrorCode) {
	t.Helper()
	require.NotEqual(t, s3err.ErrNone, code, "a grant without a documented permission must be rejected")
	require.Equal(t, http.StatusBadRequest, s3err.GetAPIError(code).HTTPStatusCode,
		"an invalid grant permission is a client error, got %v", code)
}

// TestValidateAndTransferGrantsRejectsMissingOrInvalidPermission: a grant whose
// Permission is omitted or is not one of the documented values is rejected,
// whatever the grantee type, and no grants are returned.
func TestValidateAndTransferGrantsRejectsMissingOrInvalidPermission(t *testing.T) {
	for granteeName, grantee := range validGrantees() {
		for _, permission := range invalidGrantPermissions {
			t.Run(granteeName+"/"+describePermission(permission), func(t *testing.T) {
				result, code := ValidateAndTransferGrants(grantPermissionAccounts{}, []*s3.Grant{
					{Grantee: grantee(), Permission: permission},
				})
				requireClientError(t, code)
				require.Empty(t, result, "a rejected ACL must not yield grants")
			})
		}
	}
}

// TestValidateAndTransferGrantsRejectsWholeListOnOneInvalidPermission: one bad
// grant rejects the whole ACL rather than storing the valid part of it.
func TestValidateAndTransferGrantsRejectsWholeListOnOneInvalidPermission(t *testing.T) {
	for _, permission := range invalidGrantPermissions {
		t.Run(describePermission(permission), func(t *testing.T) {
			grantees := validGrantees()
			result, code := ValidateAndTransferGrants(grantPermissionAccounts{}, []*s3.Grant{
				{Grantee: grantees["CanonicalUser"](), Permission: aws.String("READ")},
				{Grantee: grantees["Group"](), Permission: permission},
			})
			requireClientError(t, code)
			require.Empty(t, result)
		})
	}
}

// aclRequestBody builds a PutObjectAcl / PutBucketAcl AccessControlPolicy body
// with one grant per argument, each carrying its permission element verbatim
// ("" leaves the element out).
func aclRequestBody(permissionElements ...string) string {
	var grants strings.Builder
	for _, permissionElement := range permissionElements {
		grants.WriteString(`<Grant>` +
			`<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>alice</ID></Grantee>` +
			permissionElement +
			`</Grant>`)
	}
	return `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Owner><ID>owner-id</ID></Owner>` +
		`<AccessControlList>` + grants.String() + `</AccessControlList>` +
		`</AccessControlPolicy>`
}

func extractAclFromBody(body string) ([]*s3.Grant, s3err.ErrorCode) {
	r := httptest.NewRequest(http.MethodPut, "/bucket/object?acl", strings.NewReader(body))
	return ExtractAcl(r, grantPermissionAccounts{}, "", "owner-id", "owner-id", "owner-id")
}

// TestExtractAclBodyRejectsMissingOrInvalidPermission: the XML decoder does not
// check the Permission enum, so the body must be rejected by the ACL validation.
func TestExtractAclBodyRejectsMissingOrInvalidPermission(t *testing.T) {
	cases := map[string]string{
		"omitted Permission element": aclRequestBody(""),
		"empty Permission element":   aclRequestBody("<Permission></Permission>"),
		"self-closing Permission":    aclRequestBody("<Permission/>"),
		"xsi:nil Permission":         aclRequestBody(`<Permission xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true"/>`),
		"lowercase value":            aclRequestBody("<Permission>read</Permission>"),
		"unknown value":              aclRequestBody("<Permission>READ_WRITE</Permission>"),
		"padded value":               aclRequestBody("<Permission> READ </Permission>"),
		"valid then omitted":         aclRequestBody("<Permission>READ</Permission>", ""),
		"valid then unknown":         aclRequestBody("<Permission>READ</Permission>", "<Permission>ALL</Permission>"),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			grants, code := extractAclFromBody(body)
			requireClientError(t, code)
			require.Empty(t, grants)
		})
	}
}

// buildAccessControlListSafely runs the GetObjectAcl / GetBucketAcl response
// builder and turns a panic into a test failure instead of a crashed binary.
func buildAccessControlListSafely(t *testing.T, grants []*s3.Grant) (acl AccessControlList) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("building the ACL response panicked on a grant without a permission: %v", recovered)
		}
	}()
	return buildAccessControlList(grantPermissionAccounts{}, grants, "owner-id", "Owner")
}

func requireDocumentedPermissions(t *testing.T, acl AccessControlList) {
	t.Helper()
	for _, grant := range acl.Grant {
		require.Contains(t, documentedGrantPermissions, string(grant.Permission),
			"a GetAcl response may only carry documented permissions")
	}
}

// TestPutAclWithoutPermissionThenGetAcl follows a grant that omits Permission
// through the real write and read path: ExtractAcl (PutObjectAcl), storage in
// the entry, and buildAccessControlList (GetObjectAcl). Rejecting it on write
// passes; accepting it is only correct if the later read still works.
func TestPutAclWithoutPermissionThenGetAcl(t *testing.T) {
	grants, code := extractAclFromBody(aclRequestBody(""))
	if code != s3err.ErrNone {
		return
	}
	entry := &filer_pb.Entry{}
	require.Equal(t, s3err.ErrNone, AssembleEntryWithAcp(entry, "owner-id", grants))
	acl := buildAccessControlListSafely(t, GetAcpGrants(entry.Extended))
	requireDocumentedPermissions(t, acl)
}

// TestGetAclToleratesStoredGrantWithoutPermission: entries written while the
// permission went unchecked can already hold such a grant, so reading the ACL
// back must not crash and must still return the valid grants next to it.
func TestGetAclToleratesStoredGrantWithoutPermission(t *testing.T) {
	stored, err := json.Marshal([]*s3.Grant{
		{Grantee: &s3.Grantee{Type: aws.String(s3_constants.GrantTypeCanonicalUser), ID: aws.String("alice")}},
		{Grantee: &s3.Grantee{Type: aws.String(s3_constants.GrantTypeCanonicalUser), ID: aws.String("owner-id")}, Permission: aws.String("FULL_CONTROL")},
	})
	require.NoError(t, err)
	acl := buildAccessControlListSafely(t, GetAcpGrants(map[string][]byte{s3_constants.ExtAmzAclKey: stored}))
	requireDocumentedPermissions(t, acl)
	found := false
	for _, grant := range acl.Grant {
		if grant.Grantee.ID == "owner-id" && string(grant.Permission) == "FULL_CONTROL" {
			found = true
		}
	}
	require.True(t, found, "the valid grant stored next to the bad one must still be returned")
}
