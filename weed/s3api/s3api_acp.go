package s3api

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3acl"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"net/http"
)

func getAccountId(r *http.Request) string {
	id := r.Header.Get(s3_constants.AmzAccountId)
	if len(id) == 0 {
		return s3account.AccountAnonymous.Id
	} else {
		return id
	}
}

func (s3a *S3ApiServer) checkAccessByOwnership(r *http.Request, bucket string) s3err.ErrorCode {
	metadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}
	accountId := getAccountId(r)
	if accountId == s3account.AccountAdmin.Id || accountId == *metadata.Owner.ID {
		return s3err.ErrNone
	}
	return s3err.ErrAccessDenied
}

func (s3a *S3ApiServer) ExtractBucketAcp(r *http.Request) (owner string, grants []*s3.Grant, errCode s3err.ErrorCode) {
	accountId := s3acl.GetAccountId(r)

	ownership := s3_constants.DefaultOwnershipForCreate
	if ownership == s3_constants.OwnershipBucketOwnerEnforced {
		return accountId, []*s3.Grant{
			{
				Permission: &s3_constants.PermissionFullControl,
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeCanonicalUser,
					ID:   &accountId,
				},
			},
		}, s3err.ErrNone
	} else {
		return s3acl.ParseAndValidateAclHeadersOrElseDefault(r, s3a.accountManager, ownership, accountId, accountId, false)
	}
}
