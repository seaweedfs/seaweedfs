package s3_constants

import (
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	PublicRead = []*s3.Grant{
		{
			Grantee: &s3.Grantee{
				Type: &GrantTypeGroup,
				URI:  &GranteeGroupAllUsers,
			},
			Permission: &PermissionRead,
		},
	}

	PublicReadWrite = []*s3.Grant{
		{
			Grantee: &s3.Grantee{
				Type: &GrantTypeGroup,
				URI:  &GranteeGroupAllUsers,
			},
			Permission: &PermissionRead,
		},
		{
			Grantee: &s3.Grantee{
				Type: &GrantTypeGroup,
				URI:  &GranteeGroupAllUsers,
			},
			Permission: &PermissionWrite,
		},
	}

	AuthenticatedRead = []*s3.Grant{
		{
			Grantee: &s3.Grantee{
				Type: &GrantTypeGroup,
				URI:  &GranteeGroupAuthenticatedUsers,
			},
			Permission: &PermissionRead,
		},
	}

	LogDeliveryWrite = []*s3.Grant{
		{
			Grantee: &s3.Grantee{
				Type: &GrantTypeGroup,
				URI:  &GranteeGroupLogDelivery,
			},
			Permission: &PermissionWrite,
		},
	}
)
