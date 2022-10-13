package s3api

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3acl"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

//Check ObjectAcl-Read related access
// includes:
// - GetObjectAclHandler
func (s3a *S3ApiServer) checkAccessForReadObjectAcl(r *http.Request, bucket, object string) (acp *s3.AccessControlPolicy, errCode s3err.ErrorCode) {
	bucketMetadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	getAcpFunc := func() (*s3.AccessControlPolicy, s3err.ErrorCode) {
		entry, err := getObjectEntry(s3a, bucket, object)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil, s3err.ErrNoSuchKey
			} else {
				return nil, s3err.ErrInternalError
			}
		}

		if entry.IsDirectory {
			return nil, s3err.ErrExistingObjectIsDirectory
		}

		acpOwnerId := s3acl.GetAcpOwner(entry.Extended, *bucketMetadata.Owner.ID)
		acpOwnerName := s3a.accountManager.IdNameMapping[acpOwnerId]
		acpGrants := s3acl.GetAcpGrants(entry.Extended)
		acp = &s3.AccessControlPolicy{
			Owner: &s3.Owner{
				ID:          &acpOwnerId,
				DisplayName: &acpOwnerName,
			},
			Grants: acpGrants,
		}
		return acp, s3err.ErrNone
	}

	if bucketMetadata.ObjectOwnership == s3_constants.OwnershipBucketOwnerEnforced {
		return getAcpFunc()
	} else {
		accountId := s3acl.GetAccountId(r)

		acp, errCode := getAcpFunc()
		if errCode != s3err.ErrNone {
			return nil, errCode
		}

		if accountId == *acp.Owner.ID {
			return acp, s3err.ErrNone
		}

		//find in Grants
		if acp.Grants != nil {
			reqGrants := s3acl.DetermineReqGrants(accountId, s3_constants.PermissionReadAcp)
			for _, requiredGrant := range reqGrants {
				for _, grant := range acp.Grants {
					if s3acl.GrantEquals(requiredGrant, grant) {
						return acp, s3err.ErrNone
					}
				}
			}
		}

		glog.V(3).Infof("acl denied! request account id: %s", accountId)
		return nil, s3err.ErrAccessDenied
	}
}

func getObjectEntry(s3a *S3ApiServer, bucket, object string) (*filer_pb.Entry, error) {
	return s3a.getEntry(util.Join(s3a.option.BucketsPath, bucket), object)
}
