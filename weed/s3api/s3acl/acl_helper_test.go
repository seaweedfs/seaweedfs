package s3acl

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"io"
	"net/http"
	"testing"
)

var (
	accountManager = &s3account.AccountManager{
		IdNameMapping: map[string]string{
			s3account.AccountAdmin.Id:     s3account.AccountAdmin.Name,
			s3account.AccountAnonymous.Id: s3account.AccountAnonymous.Name,
			"accountA":                    "accountA",
			"accountB":                    "accountB",
		},
		EmailIdMapping: map[string]string{
			s3account.AccountAdmin.EmailAddress:     s3account.AccountAdmin.Id,
			s3account.AccountAnonymous.EmailAddress: s3account.AccountAnonymous.Id,
			"accountA@example.com":                  "accountA",
			"accountBexample.com":                   "accountB",
		},
	}
)

func TestGetAccountId(t *testing.T) {
	req := &http.Request{
		Header: make(map[string][]string),
	}
	//case1
	//accountId: "admin"
	req.Header.Set(s3_constants.AmzAccountId, s3account.AccountAdmin.Id)
	if GetAccountId(req) != s3account.AccountAdmin.Id {
		t.Fatal("expect accountId: admin")
	}

	//case2
	//accountId: "anoymous"
	req.Header.Set(s3_constants.AmzAccountId, s3account.AccountAnonymous.Id)
	if GetAccountId(req) != s3account.AccountAnonymous.Id {
		t.Fatal("expect accountId: anonymous")
	}

	//case3
	//accountId is nil => "anonymous"
	req.Header.Del(s3_constants.AmzAccountId)
	if GetAccountId(req) != s3account.AccountAnonymous.Id {
		t.Fatal("expect accountId: anonymous")
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
		accountId := s3account.AccountAnonymous.Id
		reqPermission := s3_constants.PermissionRead

		resultGrants := DetermineRequiredGrants(accountId, reqPermission)
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

		resultGrants := DetermineRequiredGrants(accountId, reqPermission)
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
				ID:   &s3account.AccountAdmin.Id,
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

	resultGrants := GetAcpGrants(nil, entry.Extended)
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

	resultGrants = GetAcpGrants(nil, entry.Extended)
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
				ID:           &s3account.AccountAdmin.Id,
				EmailAddress: &s3account.AccountAdmin.EmailAddress,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				ID: &s3account.AccountAdmin.Id,
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
				ID:   &s3account.AccountAdmin.Id,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   &s3account.AccountAdmin.Id,
			},
		}): true,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   &s3account.AccountAdmin.Id,
				URI:  &s3_constants.GranteeGroupAllUsers,
			},
		}, &s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   &s3account.AccountAdmin.Id,
			},
		}): false,

		GrantEquals(&s3.Grant{
			Permission: &s3_constants.PermissionRead,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				ID:   &s3account.AccountAdmin.Id,
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
				ID:   &s3account.AccountAdmin.Id,
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

func TestGrantWithFullControl(t *testing.T) {
	accountId := "Accountaskdfj"
	expect := &s3.Grant{
		Permission: &s3_constants.PermissionFullControl,
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeCanonicalUser,
			ID:   &accountId,
		},
	}

	result := GrantWithFullControl(accountId)

	if !GrantEquals(result, expect) {
		t.Fatal("GrantWithFullControl not expect")
	}
}

func TestExtractObjectAcl(t *testing.T) {
	type Case struct {
		id                           string
		resultErrCode, expectErrCode s3err.ErrorCode
		resultGrants, expectGrants   []*s3.Grant
		resultOwnerId, expectOwnerId string
	}
	testCases := make([]*Case, 0)
	accountAdminId := "admin"

	//Request body to specify AccessControlList
	{
		//ownership: ObjectWriter
		//s3:PutObjectAcl('createObject' is set to false), config acl through request body
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
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-ObjectWriter, createObject-false, acl-requestBody",
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
			ownerId, accountAdminId,
		})
	}
	{
		//ownership: BucketOwnerEnforced (extra acl is not allowed)
		//s3:PutObjectAcl('createObject' is set to false), config acl through request body
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
		requestAccountId := "accountA"
		_, _, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, accountAdminId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractObjectAcl: ownership-BucketOwnerEnforced, createObject-false, acl-requestBody",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}
	{
		//ownership: ObjectWriter
		//s3:PutObject('createObject' is set to false), request body will be ignored when parse acl
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
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-ObjectWriter, createObject-true, acl-requestBody",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerEnforced (extra acl is not allowed)
		//s3:PutObject('createObject' is set to true), request body will be ignored when parse acl
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
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, accountAdminId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-BucketOwnerEnforced, createObject-true, acl-requestBody",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{},
			ownerId, "",
		})
	}

	//CannedAcl Header to specify ACL
	//cannedAcl, putObjectACL
	{
		//ownership: ObjectWriter
		//s3:PutObjectACL('createObject' is set to false), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-ObjectWriter, createObject-false, acl-CannedAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutObjectACL('createObject' is set to false), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-BucketOwnerPreferred, createObject-false, acl-CannedAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutObjectACL('createObject' is set to false), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, _, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractObjectAcl: ownership-BucketOwnerEnforced, createObject-false, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}
	//cannedACL, putObject
	{
		//ownership: ObjectWriter
		//s3:PutObject('createObject' is set to true), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-ObjectWriter, createObject-true, acl-CannedAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutObject('createObject' is set to true), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-BucketOwnerPreferred, createObject-true, acl-CannedAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutObject('createObject' is set to true), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, _, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			id:            "TestExtractObjectAcl: ownership-BucketOwnerEnforced, createObject-true, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}

	//cannedAcl, putObjectACL
	{
		//ownership: ObjectWriter
		//s3:PutObjectACL('createObject' is set to false), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-ObjectWriter, createObject-false, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutObjectACL('createObject' is set to false), parse customAcl  header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-BucketOwnerPreferred, createObject-false, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutObjectACL('createObject' is set to false), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, _, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractObjectAcl: ownership-BucketOwnerEnforced, createObject-false, acl-customAcl",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}
	//customAcl, putObject
	{
		//ownership: ObjectWriter
		//s3:PutObject('createObject' is set to true), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-ObjectWriter, createObject-true, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutObject('createObject' is set to true), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		ownerId, grants, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractObjectAcl: ownership-BucketOwnerPreferred, createObject-true, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
			ownerId, "",
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutObject('createObject' is set to true), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, _, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			id:            "TestExtractObjectAcl: ownership-BucketOwnerEnforced, createObject-true, acl-customAcl",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}

	{
		//parse acp from request header: both canned acl and custom acl not allowed
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=admin, id=\"accountA\"")
		req.Header.Set(s3_constants.AmzCannedAcl, "private")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, _, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "Only one of cannedAcl, customAcl is allowed",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidRequest,
		})
	}

	{
		//Acl can only be specified in one of requestBody, cannedAcl, customAcl, simultaneous use is not allowed
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=admin, id=\"accountA\"")
		req.Header.Set(s3_constants.AmzCannedAcl, "private")
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
		requestAccountId := "accountA"
		_, _, errCode := ExtractObjectAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "Only one of requestBody, cannedAcl, customAcl is allowed",
			resultErrCode: errCode, expectErrCode: s3err.ErrUnexpectedContent,
		})
	}
	for _, tc := range testCases {
		if tc.resultErrCode != tc.expectErrCode {
			t.Fatalf("case[%s]: errorCode[%v] not expect[%v]", tc.id, s3err.GetAPIError(tc.resultErrCode).Code, s3err.GetAPIError(tc.expectErrCode).Code)
		}
		if !grantsEquals(tc.resultGrants, tc.expectGrants) {
			t.Fatalf("case[%s]: grants not expect", tc.id)
		}
	}
}

func TestBucketObjectAcl(t *testing.T) {
	type Case struct {
		id                           string
		resultErrCode, expectErrCode s3err.ErrorCode
		resultGrants, expectGrants   []*s3.Grant
	}
	testCases := make([]*Case, 0)
	accountAdminId := "admin"

	//Request body to specify AccessControlList
	{
		//ownership: ObjectWriter
		//s3:PutBucketAcl('createObject' is set to false), config acl through request body
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
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		grants, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractBucketAcl: ownership-ObjectWriter, createObject-false, acl-requestBody",
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
		//ownership: BucketOwnerEnforced (extra acl is not allowed)
		//s3:PutBucketAcl('createObject' is set to false), config acl through request body
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
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, accountAdminId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerEnforced, createObject-false, acl-requestBody",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}
	{
		//ownership: ObjectWriter
		//s3:PutObject('createObject' is set to false), request body will be ignored when parse acl
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
		requestAccountId := "accountA"
		grants, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractBucketAcl: ownership-ObjectWriter, createObject-true, acl-requestBody",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{},
		})
	}
	{
		//ownership: BucketOwnerEnforced (extra acl is not allowed)
		//s3:PutObject('createObject' is set to true), request body will be ignored when parse acl
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
		requestAccountId := "accountA"
		grants, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, accountAdminId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractBucketAcl: ownership-BucketOwnerEnforced, createObject-true, acl-requestBody",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{},
		})
	}

	//CannedAcl Header to specify ACL
	//cannedAcl, PutBucketAcl
	{
		//ownership: ObjectWriter
		//s3:PutBucketAcl('createObject' is set to false), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-ObjectWriter, createObject-false, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidAclArgument,
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutBucketAcl('createObject' is set to false), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerPreferred, createObject-false, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidAclArgument,
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutBucketAcl('createObject' is set to false), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerEnforced, createObject-false, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidAclArgument,
		})
	}
	//cannedACL, createBucket
	{
		//ownership: ObjectWriter
		//s3:PutObject('createObject' is set to true), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerEnforced, createObject-true, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidAclArgument,
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutObject('createObject' is set to true), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerPreferred, createObject-true, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidAclArgument,
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutObject('createObject' is set to true), parse cannedACL header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclBucketOwnerFullControl)
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerEnforced, createObject-true, acl-CannedAcl",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidAclArgument,
		})
	}

	//customAcl, PutBucketAcl
	{
		//ownership: ObjectWriter
		//s3:PutBucketAcl('createObject' is set to false), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		grants, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractBucketAcl: ownership-ObjectWriter, createObject-false, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutBucketAcl('createObject' is set to false), parse customAcl  header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		grants, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			"TestExtractBucketAcl: ownership-BucketOwnerPreferred, createObject-false, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutBucketAcl('createObject' is set to false), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerEnforced, createObject-false, acl-customAcl",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}
	//customAcl, putObject
	{
		//ownership: ObjectWriter
		//s3:PutObject('createObject' is set to true), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		grants, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractBucketAcl: ownership-ObjectWriter, createObject-true, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &requestAccountId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}
	{
		//ownership: BucketOwnerPreferred
		//s3:PutObject('createObject' is set to true), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		grants, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerPreferred, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			"TestExtractBucketAcl: ownership-BucketOwnerPreferred, createObject-true, acl-customAcl",
			errCode, s3err.ErrNone,
			grants, []*s3.Grant{
				{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionFullControl,
				},
			},
		})
	}
	{
		//ownership: BucketOwnerEnforced
		//s3:PutObject('createObject' is set to true), parse customAcl header
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=accountA,id=\"admin\"")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipBucketOwnerEnforced, bucketOwnerId, requestAccountId, true)
		testCases = append(testCases, &Case{
			id:            "TestExtractBucketAcl: ownership-BucketOwnerEnforced, createObject-true, acl-customAcl",
			resultErrCode: errCode, expectErrCode: s3err.AccessControlListNotSupported,
		})
	}

	{
		//parse acp from request header: both canned acl and custom acl not allowed
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=admin, id=\"accountA\"")
		req.Header.Set(s3_constants.AmzCannedAcl, "private")
		bucketOwnerId := "admin"
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, bucketOwnerId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "Only one of cannedAcl, customAcl is allowed",
			resultErrCode: errCode, expectErrCode: s3err.ErrInvalidRequest,
		})
	}

	{
		//Acl can only be specified in one of requestBody, cannedAcl, customAcl, simultaneous use is not allowed
		req := &http.Request{
			Header: make(map[string][]string),
		}
		req.Header.Set(s3_constants.AmzAclFullControl, "id=admin, id=\"accountA\"")
		req.Header.Set(s3_constants.AmzCannedAcl, "private")
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
		requestAccountId := "accountA"
		_, errCode := ExtractBucketAcl(req, accountManager, s3_constants.OwnershipObjectWriter, accountAdminId, requestAccountId, false)
		testCases = append(testCases, &Case{
			id:            "Only one of requestBody, cannedAcl, customAcl is allowed",
			resultErrCode: errCode, expectErrCode: s3err.ErrUnexpectedContent,
		})
	}
	for _, tc := range testCases {
		if tc.resultErrCode != tc.expectErrCode {
			t.Fatalf("case[%s]: errorCode[%v] not expect[%v]", tc.id, s3err.GetAPIError(tc.resultErrCode).Code, s3err.GetAPIError(tc.expectErrCode).Code)
		}
		if !grantsEquals(tc.resultGrants, tc.expectGrants) {
			t.Fatalf("case[%s]: grants not expect", tc.id)
		}
	}
}

func TestMarshalGrantsToJson(t *testing.T) {
	//ok
	bucketOwnerId := "admin"
	requestAccountId := "accountA"
	grants := []*s3.Grant{
		{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   &requestAccountId,
			},
			Permission: &s3_constants.PermissionFullControl,
		},
		{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   &bucketOwnerId,
			},
			Permission: &s3_constants.PermissionFullControl,
		},
	}
	result, err := MarshalGrantsToJson(grants)
	if err != nil {
		t.Error(err)
	}

	var grants2 []*s3.Grant
	err = json.Unmarshal(result, &grants2)
	if err != nil {
		t.Error(err)
	}

	print(string(result))
	if !grantsEquals(grants, grants2) {
		t.Fatal("grants not equal", grants, grants2)
	}

	//ok
	result, err = MarshalGrantsToJson(nil)
	if result != nil && err != nil {
		t.Fatal("error: result, err = MarshalGrantsToJson(nil)")
	}
}
