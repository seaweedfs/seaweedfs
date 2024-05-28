package s3api

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"io"
	"net/http"
	"testing"
)

var accountManager *IdentityAccessManagement

func init() {
	accountManager = &IdentityAccessManagement{}
	_ = accountManager.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Accounts: []*iam_pb.Account{
			{
				Id:           "accountA",
				DisplayName:  "accountAName",
				EmailAddress: "accountA@example.com",
			},
			{
				Id:           "accountB",
				DisplayName:  "accountBName",
				EmailAddress: "accountB@example.com",
			},
		},
	})
}

func TestGetAccountId(t *testing.T) {
	req := &http.Request{
		Header: make(map[string][]string),
	}
	//case1
	//accountId: "admin"
	req.Header.Set(s3_constants.AmzAccountId, s3_constants.AccountAdminId)
	if GetAccountId(req) != s3_constants.AccountAdminId {
		t.Fatal("expect accountId: admin")
	}

	//case2
	//accountId: "anoymous"
	req.Header.Set(s3_constants.AmzAccountId, s3_constants.AccountAnonymousId)
	if GetAccountId(req) != s3_constants.AccountAnonymousId {
		t.Fatal("expect accountId: anonymous")
	}

	//case3
	//accountId is nil => "anonymous"
	req.Header.Del(s3_constants.AmzAccountId)
	if GetAccountId(req) != s3_constants.AccountAnonymousId {
		t.Fatal("expect accountId: anonymous")
	}
}

func TestExtractAcl(t *testing.T) {
	type Case struct {
		id                           int
		resultErrCode, expectErrCode s3err.ErrorCode
		resultGrants, expectGrants   []*s3.Grant
	}
	testCases := make([]*Case, 0)
	accountAdminId := "admin"
	{
		//case1 (good case)
		//parse acp from request body
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Body = io.NopCloser(bytes.NewReader([]byte(`
	<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>admin</ID>
			<DisplayName>admin</DisplayName>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>admin</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
					<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>
	`)))
		objectWriter := "accountA"
		grants, errCode := ExtractAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, accountAdminId, objectWriter)
		testCases = append(testCases, &Case{
			1,
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &accountAdminId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeGroup,
						URI:  &s3_constants.GranteeGroupAllUsers,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}

	{
		//case2 (good case)
		//parse acp from header (cannedAcl)
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Body = nil
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclPrivate)
		objectWriter := "accountA"
		grants, errCode := ExtractAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, accountAdminId, objectWriter)
		testCases = append(testCases, &Case{
			2,
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &objectWriter,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}

	{
		//case3 (bad case)
		//parse acp from request body (content is invalid)
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Body = io.NopCloser(bytes.NewReader([]byte("zdfsaf")))
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclPrivate)
		objectWriter := "accountA"
		_, errCode := ExtractAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, accountAdminId, objectWriter)
		testCases = append(testCases, &Case{
			id:            3,
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidRequest,
		})
	}

	//case4 (bad case)
	//parse acp from header (cannedAcl is invalid)
	req := &http.Request{
		Header: make(map[string][]string),
	}
	req.Body = nil
	req.Header.Set(s3_constants.AmzCannedAcl, "dfaksjfk")
	objectWriter := "accountA"
	_, errCode := ExtractAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, "", objectWriter)
	testCases = append(testCases, &Case{
		id:            4,
		resultErrCode: errCode, expectErrCode: s3err.ErrInvalidRequest,
	})

	{
		//case5 (bad case)
		//parse acp from request body: owner is inconsistent
		req.Body = io.NopCloser(bytes.NewReader([]byte(`
	<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>admin</ID>
			<DisplayName>admin</DisplayName>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>admin</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
					<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>
	`)))
		objectWriter = "accountA"
		_, errCode := ExtractAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, objectWriter, objectWriter)
		testCases = append(testCases, &Case{
			id:            5,
			resultErrCode: errCode, expectErrCode: s3err.ErrAccessDenied,
		})
	}

	for _, tc := range testCases {
		if tc.resultErrCode != tc.expectErrCode {
			t.Fatalf("case[%d]: errorCode not expect", tc.id)
		}
		if !grantsEquals(tc.resultGrants, tc.expectGrants) {
			t.Fatalf("case[%d]: grants not expect", tc.id)
		}
	}
}

func TestParseAndValidateAclHeaders(t *testing.T) {
	type Case struct {
		id                           int
		resultOwner, expectOwner     string
		resultErrCode, expectErrCode s3err.ErrorCode
		resultGrants, expectGrants   []*s3.Grant
	}
	testCases := make([]*Case, 0)
	bucketOwner := "admin"

	{
		//case1 (good case)
		//parse custom acl
		req := &http.Request{
			Header: make(map[string][]string),
		}
		objectWriter := "accountA"
		req.Header.Set(s3_constants.AmzAclFullControl, `uri="http://acs.amazonaws.com/groups/global/AllUsers", id="anonymous", emailAddress="admin@example.com"`)
		ownerId, grants, errCode := ParseAndValidateAclHeaders(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwner, objectWriter, false)
		testCases = append(testCases, &Case{
			1,
			ownerId, objectWriter,
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeGroup,
						URI:  &s3_constants.GranteeGroupAllUsers,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   aws.String(s3_constants.AccountAnonymousId),
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   aws.String(s3_constants.AccountAdminId),
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}
	{
		//case2 (good case)
		//parse canned acl (ownership=ObjectWriter)
		req := &http.Request{
			Header: make(map[string][]string),
		}
		objectWriter := "accountA"
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		ownerId, grants, errCode := ParseAndValidateAclHeaders(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwner, objectWriter, false)
		testCases = append(testCases, &Case{
			2,
			ownerId, objectWriter,
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &objectWriter,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwner,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}
	{
		//case3 (good case)
		//parse canned acl (ownership=OwnershipBucketOwnerPreferred)
		req := &http.Request{
			Header: make(map[string][]string),
		}
		objectWriter := "accountA"
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		ownerId, grants, errCode := ParseAndValidateAclHeaders(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwner, objectWriter, false)
		testCases = append(testCases, &Case{
			3,
			ownerId, bucketOwner,
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwner,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}
	{
		//case4 (bad case)
		//parse custom acl (grantee id not exists)
		req := &http.Request{
			Header: make(map[string][]string),
		}
		objectWriter := "accountA"
		req.Header.Set(s3_constants.AmzAclFullControl, `uri="http://acs.amazonaws.com/groups/global/AllUsers", id="notExistsAccount", emailAddress="admin@example.com"`)
		_, _, errCode := ParseAndValidateAclHeaders(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwner, objectWriter, false)
		testCases = append(testCases, &Case{
			id:            4,
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidRequest,
		})
	}

	{
		//case5 (bad case)
		//parse custom acl (invalid format)
		req := &http.Request{
			Header: make(map[string][]string),
		}
		objectWriter := "accountA"
		req.Header.Set(s3_constants.AmzAclFullControl, `uri="http:sfasf"`)
		_, _, errCode := ParseAndValidateAclHeaders(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwner, objectWriter, false)
		testCases = append(testCases, &Case{
			id:            5,
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidRequest,
		})
	}

	{
		//case6 (bad case)
		//parse canned acl (invalid value)
		req := &http.Request{
			Header: make(map[string][]string),
		}
		objectWriter := "accountA"
		req.Header.Set(s3_constants.AmzCannedAcl, `uri="http:sfasf"`)
		_, _, errCode := ParseAndValidateAclHeaders(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwner, objectWriter, false)
		testCases = append(testCases, &Case{
			id:            5,
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidRequest,
		})
	}

	for _, tc := range testCases {
		if tc.expectErrCode != tc.resultErrCode {
			t.Errorf("case[%d]: errCode unexpect", tc.id)
		}
		if tc.resultOwner != tc.expectOwner {
			t.Errorf("case[%d]: ownerId unexpect", tc.id)
		}
		if !grantsEquals(tc.resultGrants, tc.expectGrants) {
			t.Fatalf("case[%d]: grants not expect", tc.id)
		}
	}
}

func grantsEquals(a, b []*s3.Grant) bool {
	if len(a) != len(b) {
		return false
	}
	for i, grant := range a {
		if !GrantEquals(grant, b[i]) {
			return false
		}
	}
	return true
}

func TestDetermineReqGrants(t *testing.T) {
	{
		//case1: request account is anonymous
		accountId := s3_constants.AccountAnonymousId
		reqPermission := s3_constants.PermissionRead

		resultGrants := DetermineReqGrants(accountId, reqPermission)
		expectGrants := []*s3.Grant{
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeGroup,
					URI:  &s3_constants.GranteeGroupAllUsers,
				},
				Permission: &reqPermission,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeGroup,
					URI:  &s3_constants.GranteeGroupAllUsers,
				},
				Permission: &s3_constants.PermissionFullControl,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeCanonicalUser,
					ID:   &accountId,
				},
				Permission: &reqPermission,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeCanonicalUser,
					ID:   &accountId,
				},
				Permission: &s3_constants.PermissionFullControl,
			},
		}
		if !grantsEquals(resultGrants, expectGrants) {
			t.Fatalf("grants not expect")
		}
	}
	{
		//case2: request account is not anonymous (Iam authed)
		accountId := "accountX"
		reqPermission := s3_constants.PermissionRead

		resultGrants := DetermineReqGrants(accountId, reqPermission)
		expectGrants := []*s3.Grant{
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeGroup,
					URI:  &s3_constants.GranteeGroupAllUsers,
				},
				Permission: &reqPermission,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeGroup,
					URI:  &s3_constants.GranteeGroupAllUsers,
				},
				Permission: &s3_constants.PermissionFullControl,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeCanonicalUser,
					ID:   &accountId,
				},
				Permission: &reqPermission,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeCanonicalUser,
					ID:   &accountId,
				},
				Permission: &s3_constants.PermissionFullControl,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeGroup,
					URI:  &s3_constants.GranteeGroupAuthenticatedUsers,
				},
				Permission: &reqPermission,
			},
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeGroup,
					URI:  &s3_constants.GranteeGroupAuthenticatedUsers,
				},
				Permission: &s3_constants.PermissionFullControl,
			},
		}
		if !grantsEquals(resultGrants, expectGrants) {
			t.Fatalf("grants not expect")
		}
	}
}

func TestAssembleEntryWithAcp(t *testing.T) {
	defaultOwner := "admin"

	//case1
	//assemble with non-empty grants
	expectOwner := "accountS"
	expectGrants := []*s3.Grant{
		{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   aws.String(s3_constants.AccountAdminId),
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		},
	}
	entry := &filer_pb.Entry{}
	AssembleEntryWithAcp(entry, expectOwner, expectGrants)

	resultOwner := GetAcpOwner(entry.Extended, defaultOwner)
	if resultOwner != expectOwner {
		t.Fatalf("owner not expect")
	}

	resultGrants := GetAcpGrants(entry.Extended)
	if !grantsEquals(resultGrants, expectGrants) {
		t.Fatal("grants not expect")
	}

	//case2
	//assemble with empty grants (override)
	AssembleEntryWithAcp(entry, "", nil)
	resultOwner = GetAcpOwner(entry.Extended, defaultOwner)
	if resultOwner != defaultOwner {
		t.Fatalf("owner not expect")
	}

	resultGrants = GetAcpGrants(entry.Extended)
	if len(resultGrants) != 0 {
		t.Fatal("grants not expect")
	}

}

func TestGrantEquals(t *testing.T) {
	testCases := map[bool]bool{
		GrantEquals(nil, nil): true,

		GrantEquals(&s3.Grant{}, nil): false,

		GrantEquals(&s3.Grant{}, &s3.Grant{}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
		}, &s3.Grant{}): false,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
		}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee:    &s3.Grantee{},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee:    &s3.Grantee{},
		}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee:    &s3.Grantee{},
		}): false,

		//type not present, compare other fields of grant is meaningless
		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				ID: aws.String(s3_constants.AccountAdminId),
				//EmailAddress: &s3account.AccountAdmin.EmailAddress,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				ID: aws.String(s3_constants.AccountAdminId),
			},
		}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
			},
		}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionWrite,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}): false,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   aws.String(s3_constants.AccountAdminId),
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   aws.String(s3_constants.AccountAdminId),
			},
		}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   aws.String(s3_constants.AccountAdminId),
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   aws.String(s3_constants.AccountAdminId),
			},
		}): false,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   aws.String(s3_constants.AccountAdminId),
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}): true,
	}

	for tc, expect := range testCases {
		if tc != expect {
			t.Fatal("TestGrantEquals not expect!")
		}
	}
}

func TestSetAcpOwnerHeader(t *testing.T) {
	ownerId := "accountZ"
	req := &http.Request{
		Header: make(map[string][]string),
	}
	SetAcpOwnerHeader(req, ownerId)

	if req.Header.Get(s3_constants.ExtAmzOwnerKey) != ownerId {
		t.Fatalf("owner unexpect")
	}
}

func TestSetAcpGrantsHeader(t *testing.T) {
	req := &http.Request{
		Header: make(map[string][]string),
	}
	grants := []*s3.Grant{
		{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   aws.String(s3_constants.AccountAdminId),
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		},
	}
	SetAcpGrantsHeader(req, grants)

	grantsJson, _ := json.Marshal(grants)
	if req.Header.Get(s3_constants.ExtAmzAclKey) != string(grantsJson) {
		t.Fatalf("owner unexpect")
	}
}
