package s3api

import (
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

// Check ObjectAcl-Write related access
// includes:
// - PutObjectAclHandler
func (s3a *S3ApiServer) checkAccessForWriteObjectAcl(accountId, bucket, object string) (bucketMetadata *BucketMetaData, objectEntry *filer_pb.Entry, objectOwner string, errCode s3err.ErrorCode) {
	bucketMetadata, errCode = s3a.bucketRegistry.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return nil, nil, "", errCode
	}

	if bucketMetadata.ObjectOwnership == s3_constants.OwnershipBucketOwnerEnforced {
		return nil, nil, "", s3err.AccessControlListNotSupported
	}

	//bucket acl
	bucketAclAllowed := false
	reqGrants := s3acl.DetermineReqGrants(accountId, s3_constants.PermissionWrite)
	if accountId == *bucketMetadata.Owner.ID {
		bucketAclAllowed = true
	} else if bucketMetadata.Acl != nil {
	bucketLoop:
		for _, bucketGrant := range bucketMetadata.Acl {
			for _, requiredGrant := range reqGrants {
				if s3acl.GrantEquals(bucketGrant, requiredGrant) {
					bucketAclAllowed = true
					break bucketLoop
				}
			}
		}
	}
	if !bucketAclAllowed {
		return nil, nil, "", s3err.ErrAccessDenied
	}

	//object acl
	objectEntry, err := getObjectEntry(s3a, bucket, object)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil, nil, "", s3err.ErrNoSuchKey
		}
		return nil, nil, "", s3err.ErrInternalError
	}

	if objectEntry.IsDirectory {
		return nil, nil, "", s3err.ErrExistingObjectIsDirectory
	}

	objectOwner = s3acl.GetAcpOwner(objectEntry.Extended, *bucketMetadata.Owner.ID)
	if accountId == objectOwner {
		return bucketMetadata, objectEntry, objectOwner, s3err.ErrNone
	}

	objectGrants := s3acl.GetAcpGrants(objectEntry.Extended)
	if objectGrants != nil {
		for _, objectGrant := range objectGrants {
			for _, requiredGrant := range reqGrants {
				if s3acl.GrantEquals(objectGrant, requiredGrant) {
					return bucketMetadata, objectEntry, objectOwner, s3err.ErrNone
				}
			}
		}
	}

	glog.V(3).Infof("acl denied! request account id: %s", accountId)
	return nil, nil, "", s3err.ErrAccessDenied
}

func getObjectEntry(s3a *S3ApiServer, bucket, object string) (*filer_pb.Entry, error) {
	return s3a.getEntry(util.Join(s3a.option.BucketsPath, bucket), object)
}

func updateObjectEntry(s3a *S3ApiServer, bucket string, entry *filer_pb.Entry) error {
	return s3a.updateEntry(util.Join(s3a.option.BucketsPath, bucket), entry)
}
