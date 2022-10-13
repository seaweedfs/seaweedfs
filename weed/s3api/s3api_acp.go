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

// Check Object-Write related access
// includes:
// - PutObjectHandler
// - PutObjectPartHandler
func (s3a *S3ApiServer) checkAccessForWriteObject(r *http.Request, bucket, object string) s3err.ErrorCode {
	bucketMetadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}

	accountId := s3acl.GetAccountId(r)
	if bucketMetadata.ObjectOwnership == s3_constants.OwnershipBucketOwnerEnforced {
		// validate grants (only bucketOwnerFullControl acl is allowed)
		_, grants, errCode := s3acl.ParseAndValidateAclHeaders(r, s3a.accountManager, bucketMetadata.ObjectOwnership, *bucketMetadata.Owner.ID, accountId, false)
		if errCode != s3err.ErrNone {
			return errCode
		}
		if len(grants) > 1 {
			return s3err.AccessControlListNotSupported
		}
		bucketOwnerFullControlGrant := &s3.Grant{
			Permission: &s3_constants.PermissionFullControl,
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   bucketMetadata.Owner.ID,
			},
		}
		if len(grants) == 0 {
			// set default grants
			s3acl.SetAcpOwnerHeader(r, accountId)
			s3acl.SetAcpGrantsHeader(r, []*s3.Grant{bucketOwnerFullControlGrant})
			return s3err.ErrNone
		}

		if !s3acl.GrantEquals(bucketOwnerFullControlGrant, grants[0]) {
			return s3err.AccessControlListNotSupported
		}

		s3acl.SetAcpOwnerHeader(r, accountId)
		s3acl.SetAcpGrantsHeader(r, []*s3.Grant{bucketOwnerFullControlGrant})
		return s3err.ErrNone
	}

	//bucket access allowed
	bucketAclAllowed := false
	if accountId == *bucketMetadata.Owner.ID {
		bucketAclAllowed = true
	} else {
		if len(bucketMetadata.Acl) > 0 {
			reqGrants := s3acl.DetermineReqGrants(accountId, s3_constants.PermissionWrite)
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
	}
	if !bucketAclAllowed {
		glog.V(3).Infof("acl denied! request account id: %s", accountId)
		return s3err.ErrAccessDenied
	}

	//object access allowed
	entry, err := getObjectEntry(s3a, bucket, object)
	if err != nil {
		if err != filer_pb.ErrNotFound {
			return s3err.ErrInternalError
		}
	} else {
		if entry.IsDirectory {
			return s3err.ErrExistingObjectIsDirectory
		}

		//Only the owner of the bucket and the owner of the object can overwrite the object
		objectOwner := s3acl.GetAcpOwner(entry.Extended, *bucketMetadata.Owner.ID)
		if accountId != objectOwner && accountId != *bucketMetadata.Owner.ID {
			glog.V(3).Infof("acl denied! request account id: %s, expect account id: %s", accountId, *bucketMetadata.Owner.ID)
			return s3err.ErrAccessDenied
		}
	}

	ownerId, grants, errCode := s3acl.ParseAndValidateAclHeadersOrElseDefault(r, s3a.accountManager, bucketMetadata.ObjectOwnership, *bucketMetadata.Owner.ID, accountId, false)
	if errCode != s3err.ErrNone {
		return errCode
	}

	s3acl.SetAcpOwnerHeader(r, ownerId)
	s3acl.SetAcpGrantsHeader(r, grants)

	return s3err.ErrNone
}

func getObjectEntry(s3a *S3ApiServer, bucket, object string) (*filer_pb.Entry, error) {
	return s3a.getEntry(util.Join(s3a.option.BucketsPath, bucket), object)
}
