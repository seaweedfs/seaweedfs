package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
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

// Check Object-Read related access
// includes:
// - GetObjectHandler
//
// offload object access validation to Filer layer
// - s3acl.CheckObjectAccessForReadObject
func (s3a *S3ApiServer) checkBucketAccessForReadObject(r *http.Request, bucket string) s3err.ErrorCode {
	bucketMetadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}

	if bucketMetadata.ObjectOwnership != s3_constants.OwnershipBucketOwnerEnforced {
		//offload object acl validation to filer layer
		r.Header.Set(s3_constants.XSeaweedFSHeaderAmzBucketOwnerId, *bucketMetadata.Owner.ID)
	}

	return s3err.ErrNone
}
