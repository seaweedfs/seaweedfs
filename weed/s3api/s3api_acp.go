package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// Check Bucket/BucketAcl Read related access
// includes:
// - HeadBucketHandler
// - GetBucketAclHandler
// - ListObjectsV1Handler
// - ListObjectsV2Handler
func (s3a *S3ApiServer) checkAccessForReadBucket(r *http.Request, bucket, aclAction string) (*BucketMetaData, s3err.ErrorCode) {
	bucketMetadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	if bucketMetadata.ObjectOwnership == s3_constants.OwnershipBucketOwnerEnforced {
		return bucketMetadata, s3err.ErrNone
	}

	accountId := s3acl.GetAccountId(r)
	if accountId == s3account.AccountAdmin.Id || accountId == *bucketMetadata.Owner.ID {
		return bucketMetadata, s3err.ErrNone
	}

	if len(bucketMetadata.Acl) > 0 {
		reqGrants := s3acl.DetermineReqGrants(accountId, aclAction)
		for _, bucketGrant := range bucketMetadata.Acl {
			for _, reqGrant := range reqGrants {
				if s3acl.GrantEquals(bucketGrant, reqGrant) {
					return bucketMetadata, s3err.ErrNone
				}
			}
		}
	}

	glog.V(3).Infof("acl denied! request account id: %s", accountId)
	return nil, s3err.ErrAccessDenied
}
