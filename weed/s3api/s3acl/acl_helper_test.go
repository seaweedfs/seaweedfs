package s3acl

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"testing"
)

func TestParseAclHeaders(t *testing.T) {
	accountManager := &s3account.AccountManager{
		IdNameMapping: map[string]string{
			s3account.AccountAdmin.Id:     s3account.AccountAdmin.Name,
			s3account.AccountAnonymous.Id: s3account.AccountAnonymous.Name,
		},
		EmailIdMapping: map[string]string{
			s3account.AccountAdmin.EmailAddress:     s3account.AccountAdmin.Id,
			s3account.AccountAnonymous.EmailAddress: s3account.AccountAnonymous.Id,
		},
	}

	//good value
	grants := make([]*s3.Grant, 0)
	validHeaderValue := `uri="http://acs.amazonaws.com/groups/global/AllUsers", id="anonymous", emailAddress="admin@example.com"`
	errCode := ParseCustomAclHeader(validHeaderValue, s3_constants.PermissionFullControl, &grants)
	if errCode != s3err.ErrNone {
		t.Fatal(errCode)
	}
	_, errCode = ValidateAndTransferGrants(accountManager, grants)
	if errCode != s3err.ErrNone {
		t.Fatal(errCode)
	}

	//bad case: acl header format error
	grants = make([]*s3.Grant, 0)
	formatErrCase := `uri, id="anonymous", emailAddress="admin@example.com"`
	errCode = ParseCustomAclHeader(formatErrCase, s3_constants.PermissionFullControl, &grants)
	if errCode != s3err.ErrInvalidRequest {
		t.Fatal(errCode)
	}

	//bad case: email not exists
	grants = make([]*s3.Grant, 0)
	badCaseOfEmail := `uri="http://acs.amazonaws.com/groups/global/AllUsers", id="anonymous", emailAddress="admin@example1.com"`
	errCode = ParseCustomAclHeader(badCaseOfEmail, s3_constants.PermissionFullControl, &grants)
	if errCode != s3err.ErrNone {
		t.Fatal(errCode)
	}
	_, errCode = ValidateAndTransferGrants(accountManager, grants)
	if errCode != s3err.ErrInvalidRequest {
		t.Fatal(errCode)
	}

	//bad case: account id not exists
	grants = make([]*s3.Grant, 0)
	badCaseOfAccountId := "uri=\"http://acs.amazonaws.com/groups/global/AllUsers\", id=\"xxxxxx\", emailAddress=\"admin@example.com\""
	errCode = ParseCustomAclHeader(badCaseOfAccountId, s3_constants.PermissionFullControl, &grants)
	if errCode != s3err.ErrNone {
		t.Fatal(errCode)
	}
	_, errCode = ValidateAndTransferGrants(accountManager, grants)
	if errCode != s3err.ErrInvalidRequest {
		t.Fatal(errCode)
	}

	//bad case: group url not valid
	grants = make([]*s3.Grant, 0)
	badCaseOfURL := "uri=\"http://acs.amazonaws.com/groups/global/AllUsers111xxxx\", id=\"anonymous\", emailAddress=\"admin@example.com\""
	errCode = ParseCustomAclHeader(badCaseOfURL, s3_constants.PermissionFullControl, &grants)
	if errCode != s3err.ErrNone {
		t.Fatal(errCode)
	}
	_, errCode = ValidateAndTransferGrants(accountManager, grants)
	if errCode != s3err.ErrInvalidRequest {
		t.Fatal(errCode)
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
