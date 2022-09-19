package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go/private/protocol/json/jsonutil"
	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"net/http"
	"reflect"
	"strings"
)

var (
	//cannedAcl
	CannedAclPrivate                = "private"
	CannedAclPublicRead             = "public-read"
	CannedAclPublicReadWrite        = "public-read-write"
	CannedAclAuthenticatedRead      = "authenticated-read"
	CannedAclLogDeliveryWrite       = "log-delivery-write"
	CannedAclBucketOwnerRead        = "bucket-owner-read"
	CannedAclBucketOwnerFullControl = "bucket-owner-full-control"
	CannedAclAwsExecRead            = "aws-exec-read"

	//ownership
	OwnershipBucketOwnerPreferred     = "BucketOwnerPreferred"
	OwnershipObjectWriter             = "ObjectWriter"
	OwnershipBucketOwnerEnforced      = "BucketOwnerEnforced"
	DefaultOwnershipForExistingBucket = OwnershipBucketOwnerEnforced
	DefaultOwnershipForCreate         = OwnershipObjectWriter

	//default Acp if absent
	DefaultAcp = &s3.AccessControlPolicy{
		Owner: &s3.Owner{
			ID:          &AccountAdmin.CanonicalId,
			DisplayName: &AccountAdmin.Name,
		},
	}
)

func (s3a *S3ApiServer) BucketReadAccess(r *http.Request, bucket, permission *string) (s3err.ErrorCode, *BucketMetaData) {
	bucketMetadata, errCode := s3a.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode, nil
	}

	if *bucketMetadata.ObjectOwnership == OwnershipBucketOwnerEnforced {
		return s3err.ErrNone, bucketMetadata
	}

	accountId := ensureAccountId(r)
	if *accountId == *bucketMetadata.Owner.ID {
		return s3err.ErrNone, bucketMetadata
	}

	if len(bucketMetadata.Acl) > 0 {
		reqGrants := s3a.determineReqGrants(accountId, permission)
		for _, bucketGrant := range bucketMetadata.Acl {
			for _, reqGrant := range reqGrants {
				if reflect.DeepEqual(bucketGrant, reqGrant) {
					return s3err.ErrNone, bucketMetadata
				}
			}
		}
	}

	return s3err.ErrAccessDenied, nil
}

func (s3a *S3ApiServer) BucketAclWriteAccess(r *http.Request, bucket *string) (s3err.ErrorCode, *BucketMetaData, *string) {
	bucketMetadata, errCode := s3a.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode, nil, nil
	}

	if *bucketMetadata.ObjectOwnership == OwnershipBucketOwnerEnforced {
		return s3err.AccessControlListNotSupported, nil, nil
	}

	accountId := ensureAccountId(r)
	if *accountId == *bucketMetadata.Owner.ID {
		return s3err.ErrNone, bucketMetadata, accountId
	}

	if len(bucketMetadata.Acl) > 0 {
		reqGrants := s3a.determineReqGrants(accountId, &s3_constants.PermissionWriteAcp)
		for _, bucketGrant := range bucketMetadata.Acl {
			for _, reqGrant := range reqGrants {
				if reflect.DeepEqual(bucketGrant, reqGrant) {
					return s3err.ErrNone, bucketMetadata, accountId
				}
			}
		}
	}

	return s3err.ErrAccessDenied, nil, nil
}

func (s3a *S3ApiServer) OwnershipAccess(r *http.Request, bucket *string) s3err.ErrorCode {
	metadata, errCode := s3a.GetBucketMetadata(bucket)
	if errCode == s3err.ErrNone {
		accountId := ensureAccountId(r)
		if *accountId == *metadata.Owner.ID {
			return s3err.ErrNone
		}
	}
	return s3err.ErrAccessDenied
}

func (s3a *S3ApiServer) ObjectWriteAccess(r *http.Request, bucket, object *string) s3err.ErrorCode {
	bucketMetadata, errCode := s3a.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}

	accountId := ensureAccountId(r)
	if *bucketMetadata.ObjectOwnership == OwnershipBucketOwnerEnforced {
		//todo: acl header should not present

		setAcpHeader(r, &s3.AccessControlPolicy{
			Owner: &s3.Owner{
				ID:          accountId,
				DisplayName: s3a.GetAccountName(accountId),
			},
		})
		return s3err.ErrNone
	}

	//bucket acl
	bucketAclAllowed := false
	if *accountId == *bucketMetadata.Owner.ID {
		bucketAclAllowed = true
	} else {
		if len(bucketMetadata.Acl) > 0 {
			reqGrants := s3a.determineReqGrants(accountId, &s3_constants.PermissionWrite)
		bucketLoop:
			for _, bucketGrant := range bucketMetadata.Acl {
				for _, requiredGrant := range reqGrants {
					if reflect.DeepEqual(bucketGrant, requiredGrant) {
						bucketAclAllowed = true
						break bucketLoop
					}
				}
			}
		}
	}
	if !bucketAclAllowed {
		return s3err.ErrAccessDenied
	}

	//object acl
	entry, err := s3a.getObjectEntry(bucket, object)
	if err != nil {
		if err != filer_pb.ErrNotFound {
			return s3err.ErrInternalError
		}
	} else if !entry.IsDirectory {
		acp := getAcp(entry)
		//Only the owner of the bucket and the owner of the object can overwrite the object
		if *accountId != *acp.Owner.ID && *accountId != *bucketMetadata.Owner.ID {
			return s3err.ErrAccessDenied
		}
	}

	//set acl header
	acp, errCode := s3a.ParseAcpFromHeaders(r, bucketMetadata.ObjectOwnership, bucketMetadata.Owner.ID, accountId, accountId, false)
	if errCode != s3err.ErrNone {
		return errCode
	}
	setAcpHeader(r, acp)

	return s3err.ErrNone
}

func (s3a *S3ApiServer) ObjectAclWriteAccess(r *http.Request, bucket, object *string) (s3err.ErrorCode, *BucketMetaData, *filer_pb.Entry, *string, *s3.AccessControlPolicy) {
	bucketMetadata, errCode := s3a.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode, nil, nil, nil, nil
	}

	if *bucketMetadata.ObjectOwnership == OwnershipBucketOwnerEnforced {
		return s3err.AccessControlListNotSupported, nil, nil, nil, nil
	}

	accountId := ensureAccountId(r)

	//bucket acl
	bucketAclAllowed := false
	var reqGrants []*s3.Grant
	if *accountId != *bucketMetadata.Owner.ID {
		reqGrants = s3a.determineReqGrants(accountId, &s3_constants.PermissionWrite)
	bucketLoop:
		for _, bucketGrant := range bucketMetadata.Acl {
			for _, requiredGrant := range reqGrants {
				if reflect.DeepEqual(bucketGrant, requiredGrant) {
					bucketAclAllowed = true
					break bucketLoop
				}
			}
		}
	}
	if !bucketAclAllowed {
		return s3err.ErrAccessDenied, nil, nil, nil, nil
	}

	//object acl
	entry, err := s3a.getObjectEntry(bucket, object)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return s3err.ErrNoSuchKey, nil, nil, nil, nil
		}
		return s3err.ErrInternalError, nil, nil, nil, nil
	}

	if entry.IsDirectory {
		return s3err.ErrNoSuchKey, nil, nil, nil, nil
	}

	acp := getAcp(entry)
	if *accountId != *acp.Owner.ID {
		if acp.Grants != nil {
			if len(reqGrants) == 0 {
				reqGrants = s3a.determineReqGrants(accountId, &s3_constants.PermissionWriteAcp)
			}
			for _, objectGrant := range acp.Grants {
				for _, requiredGrant := range reqGrants {
					if reflect.DeepEqual(objectGrant, requiredGrant) {
						return s3err.ErrNone, bucketMetadata, entry, accountId, acp
					}
				}
			}
		}
	}
	return s3err.ErrAccessDenied, nil, nil, nil, nil
}

func (s3a *S3ApiServer) ObjectReadAccess(r *http.Request, action, bucket, object *string) s3err.ErrorCode {
	bucketMetadata, errCode := s3a.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}

	if *bucketMetadata.ObjectOwnership == OwnershipBucketOwnerEnforced {
		return s3err.ErrNone
	}

	accountId := ensureAccountId(r)
	entry, err := s3a.getObjectEntry(bucket, object)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return s3err.ErrNoSuchKey
		} else {
			return s3err.ErrInternalError
		}
	}

	//ignore entry create by mkDir
	if entry.IsDirectory {
		return s3err.ErrNone
	}

	//owner access
	acp := getAcp(entry)
	if *accountId == *acp.Owner.ID {
		return s3err.ErrNone
	}

	//find in Grants
	if acp.Grants != nil {
		reqGrants := s3a.determineReqGrants(accountId, action)
		for _, requiredGrant := range reqGrants {
			for _, grant := range acp.Grants {
				if requiredGrant == grant {
					return s3err.ErrNone
				}
			}
		}
	}
	return s3err.ErrAccessDenied
}

func (s3a *S3ApiServer) ObjectAclReadAccess(r *http.Request, action, bucket, object *string) (errCode s3err.ErrorCode, acp *s3.AccessControlPolicy) {
	bucketMetadata, errCode := s3a.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode, nil
	}

	getAcpFunc := func() (s3err.ErrorCode, *s3.AccessControlPolicy) {
		entry, err := s3a.getObjectEntry(bucket, object)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return s3err.ErrNoSuchKey, nil
			} else {
				return s3err.ErrInternalError, nil
			}
		}

		//ignore
		if entry.IsDirectory {
			return s3err.ErrNoSuchKey, nil
		}

		return s3err.ErrNone, getAcp(entry)
	}

	if *bucketMetadata.ObjectOwnership == OwnershipBucketOwnerEnforced {
		return getAcpFunc()
	} else {
		accountId := ensureAccountId(r)

		errCode, acp := getAcpFunc()
		if errCode != s3err.ErrNone {
			return errCode, nil
		}

		if *accountId == *acp.Owner.ID {
			return s3err.ErrNone, acp
		}

		//find in Grants
		if acp.Grants != nil {
			reqGrants := s3a.determineReqGrants(accountId, action)
			for _, requiredGrant := range reqGrants {
				for _, grant := range acp.Grants {
					if requiredGrant == grant {
						return s3err.ErrNone, acp
					}
				}
			}
		}
		return s3err.ErrAccessDenied, nil
	}
}

// ParseAcp bucketOwnerId exists only when writeObject/writeObjectAcl is executed
func (s3a *S3ApiServer) ParseAcp(r *http.Request, ownership, bucketOwnerId, expectOwnerId, accountId *string) (*s3.AccessControlPolicy, s3err.ErrorCode) {
	if accountId == nil {
		glog.Warning("accountId is nil!")
		return nil, s3err.ErrAccessDenied
	}
	var acp s3.AccessControlPolicy
	if r.Body != nil && r.Body != http.NoBody {
		defer func(Body io.ReadCloser) {
			_ = Body.Close()
		}(r.Body)

		err := xmlutil.UnmarshalXML(&acp, xml.NewDecoder(r.Body), "")
		if err != nil {
			return nil, s3err.ErrInvalidRequest
		}

		//owner should present && owner is immutable
		if acp.Owner == nil || acp.Owner.ID == nil || expectOwnerId == nil || *acp.Owner.ID != *expectOwnerId {
			return nil, s3err.ErrInvalidRequest
		}
		return &acp, s3err.ErrNone
	} else {
		return s3a.ParseAcpFromHeaders(r, ownership, bucketOwnerId, expectOwnerId, accountId, true)
	}
}

func ensureAccountId(r *http.Request) *string {
	id := r.Header.Get(s3_constants.AmzIdentityAccountId)
	if len(id) == 0 {
		return &AccountAnonymousId
	} else {
		return &id
	}
}

func (s3a *S3ApiServer) ParseAcpFromHeaders(r *http.Request, ownership, bucketOwnerId, expectOwnerId, accountId *string, isWriteAcl bool) (*s3.AccessControlPolicy, s3err.ErrorCode) {
	var acp s3.AccessControlPolicy
	defer func() {
		GrantOwnerFullControl(&acp, accountId)
		ensureAcpOwner(&acp, expectOwnerId, s3a.GetAccountName(expectOwnerId))
	}()

	if !isWriteAcl {
		var errCode s3err.ErrorCode
		allPermission := []string{s3_constants.AmzAclFullControl, s3_constants.AmzAclRead, s3_constants.AmzAclReadAcp, s3_constants.AmzAclWrite, s3_constants.AmzAclWriteAcp}
		for _, permission := range allPermission {
			errCode = ParseAclHeader(r, &permission, &acp)
			if errCode != s3err.ErrNone {
				return nil, errCode
			}
		}
	}
	if len(acp.Grants) > 0 {
		return &acp, s3err.ErrNone
	}

	//read canned acl from header if no custom acl specified
	cannedAcl := r.Header.Get(s3_constants.AmzCannedAcl)
	if len(cannedAcl) > 0 {
		err := s3a.ParseCannedAclHeader(&acp, ownership, bucketOwnerId, accountId, &cannedAcl)
		if err != nil {
			return nil, s3err.ErrInvalidRequest
		}
	}
	return &acp, s3err.ErrNone
}

func ParseAclHeader(r *http.Request, permission *string, acp *s3.AccessControlPolicy) s3err.ErrorCode {
	hv := r.Header.Get(*permission)
	if len(hv) > 0 {
		split := strings.Split(hv, ", ")
		for _, grantStr := range split {
			kv := strings.Split(grantStr, "=")
			if len(kv) != 2 {
				return s3err.ErrInvalidRequest
			}

			switch kv[0] {
			case "id":
				acp.Grants = append(acp.Grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &kv[1],
					},
					Permission: permission,
				})
			case "emailAddress":
				acp.Grants = append(acp.Grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type:         &s3_constants.GrantTypeAmazonCustomerByEmail,
						EmailAddress: &kv[1],
					},
					Permission: permission,
				})
			case "uri":
				acp.Grants = append(acp.Grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeGroup,
						URI:  &kv[1],
					},
					Permission: permission,
				})
			}
		}
	}
	return s3err.ErrNone

}

func (s3a *S3ApiServer) ParseCannedAclHeader(acp *s3.AccessControlPolicy, bucketOwnership *string, bucketOwnerId *string, identAccountId *string, cannedAcl *string) error {
	accountFullControl := &s3.Grant{
		Grantee: &s3.Grantee{
			ID:   identAccountId,
			Type: &s3_constants.GrantTypeCanonicalUser,
		},
		Permission: &s3_constants.PermissionFullControl,
	}

	switch *cannedAcl {
	case CannedAclPrivate:
		acp.Grants = append(acp.Grants, accountFullControl)
	case CannedAclPublicRead:
		acp.Grants = append(acp.Grants, accountFullControl)
		acp.Grants = append(acp.Grants, s3_constants.PublicRead...)
	case CannedAclPublicReadWrite:
		acp.Grants = append(acp.Grants, accountFullControl)
		acp.Grants = append(acp.Grants, s3_constants.PublicReadWrite...)
	case CannedAclAuthenticatedRead:
		acp.Grants = append(acp.Grants, accountFullControl)
		acp.Grants = append(acp.Grants, s3_constants.AuthenticatedRead...)
	case CannedAclLogDeliveryWrite:
		acp.Grants = append(acp.Grants, accountFullControl)
		acp.Grants = append(acp.Grants, s3_constants.LogDeliveryWrite...)
	case CannedAclBucketOwnerRead:
		acp.Grants = append(acp.Grants, accountFullControl)
		if bucketOwnerId != nil && *bucketOwnerId != *identAccountId {
			acp.Grants = append(acp.Grants,
				&s3.Grant{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   bucketOwnerId,
					},
					Permission: &s3_constants.PermissionRead,
				})
		}
	case CannedAclBucketOwnerFullControl:
		if bucketOwnerId != nil {
			if *bucketOwnership == OwnershipBucketOwnerPreferred {
				acp.Owner = &s3.Owner{
					ID:          bucketOwnerId,
					DisplayName: s3a.GetAccountName(bucketOwnerId),
				}
				acp.Grants = append(acp.Grants,
					&s3.Grant{
						Grantee: &s3.Grantee{
							Type: &s3_constants.GrantTypeCanonicalUser,
							ID:   bucketOwnerId,
						},
						Permission: &s3_constants.PermissionFullControl,
					})
			} else {
				acp.Grants = append(acp.Grants, accountFullControl)
				if *identAccountId != *bucketOwnerId {
					acp.Grants = append(acp.Grants,
						&s3.Grant{
							Grantee: &s3.Grantee{
								Type: &s3_constants.GrantTypeCanonicalUser,
								ID:   bucketOwnerId,
							},
							Permission: &s3_constants.PermissionFullControl,
						})
				}
			}
		}
	case CannedAclAwsExecRead:
		//not implement
	default:
		return fmt.Errorf("invalid canned acl: %s", *cannedAcl)
	}
	return nil
}

func (s3a *S3ApiServer) determineReqGrants(accountId, action *string) (grants []*s3.Grant) {
	//AllUsers Group
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeGroup,
			URI:  &s3_constants.GranteeGroupAllUsers,
		},
		Permission: action,
	})
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeGroup,
			URI:  &s3_constants.GranteeGroupAllUsers,
		},
		Permission: &s3_constants.PermissionFullControl,
	})

	//CanonicalId of Anonymous
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeCanonicalUser,
			ID:   accountId,
		},
		Permission: action,
	})
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeCanonicalUser,
			ID:   accountId,
		},
		Permission: &s3_constants.PermissionFullControl,
	})

	//authenticate user grants
	if *accountId != AccountAnonymousId {
		//IAM user
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAuthenticatedUsers,
			},
			Permission: action,
		})
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAuthenticatedUsers,
			},
			Permission: &s3_constants.PermissionFullControl,
		})

		//CanonicalId
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   accountId,
			},
			Permission: action,
		})
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   accountId,
			},
			Permission: &s3_constants.PermissionFullControl,
		})
	}
	return
}

func GrantOwnerFullControl(acp *s3.AccessControlPolicy, ownerCanonicalId *string) {
	if len(acp.Grants) == 0 {
		acp.Grants = append(acp.Grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   ownerCanonicalId,
			},
			Permission: &s3_constants.PermissionFullControl,
		})
	}
}

func (s3a *S3ApiServer) getObjectEntry(bucket, object *string) (*filer_pb.Entry, error) {
	return s3a.getEntry(util.Join(s3a.Option.BucketsPath, *bucket), *object)
}

func (s3a *S3ApiServer) getBucketEntry(bucket *string) (*filer_pb.Entry, error) {
	return s3a.getEntry(s3a.Option.BucketsPath, *bucket)
}

func getAcp(entry *filer_pb.Entry) *s3.AccessControlPolicy {
	acpBytes, ok := entry.Extended[s3_constants.ExtAcpKey]
	if ok {
		var acp s3.AccessControlPolicy
		err := jsonutil.UnmarshalJSON(&acp, bytes.NewReader(acpBytes))
		if err == nil {
			return &acp
		}
	}
	return DefaultAcp
}

func setAcpHeader(r *http.Request, acp *s3.AccessControlPolicy) {
	if acp != nil {
		a, _ := jsonutil.BuildJSON(acp)
		r.Header.Set(s3_constants.ExtAcpKey, string(a))
	}
}

func ensureAcpOwner(acp *s3.AccessControlPolicy, accountId *string, accountName *string) {
	if acp.Owner == nil {
		acp.Owner = &s3.Owner{
			ID:          accountId,
			DisplayName: accountName,
		}
	}
}
