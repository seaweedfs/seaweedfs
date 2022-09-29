package s3_constants

import (
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	CannedAclPrivate                = "private"
	CannedAclPublicRead             = "public-read"
	CannedAclPublicReadWrite        = "public-read-write"
	CannedAclAuthenticatedRead      = "authenticated-read"
	CannedAclLogDeliveryWrite       = "log-delivery-write"
	CannedAclBucketOwnerRead        = "bucket-owner-read"
	CannedAclBucketOwnerFullControl = "bucket-owner-full-control"
	CannedAclAwsExecRead            = "aws-exec-read"
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
