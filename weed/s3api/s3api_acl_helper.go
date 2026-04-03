package s3api

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

type AccountManager interface {
	GetAccountNameById(canonicalId string) string
	GetAccountIdByEmail(email string) string
}

// ExtractAcl extracts the acl from the request body, or from the header if request body is empty
func ExtractAcl(r *http.Request, accountManager AccountManager, ownership, bucketOwnerId, ownerId, accountId string) (grants []*s3.Grant, errCode s3err.ErrorCode) {
	if r.Body != nil && r.Body != http.NoBody {
		defer util_http.CloseRequest(r)

		var acp s3.AccessControlPolicy
		err := xmlutil.UnmarshalXML(&acp, xml.NewDecoder(r.Body), "")
		if err != nil || acp.Owner == nil || acp.Owner.ID == nil {
			return nil, s3err.ErrInvalidRequest
		}

		//owner should present && owner is immutable
		if *acp.Owner.ID != ownerId {
			glog.V(3).Infof("set acl denied! owner account is not consistent, request account id: %s, expect account id: %s", accountId, ownerId)
			return nil, s3err.ErrAccessDenied
		}

		return ValidateAndTransferGrants(accountManager, acp.Grants)
	} else {
		_, grants, errCode = ParseAndValidateAclHeadersOrElseDefault(r, accountManager, ownership, bucketOwnerId, accountId, true)
		return grants, errCode
	}
}

// ParseAndValidateAclHeadersOrElseDefault will callParseAndValidateAclHeaders to get Grants, if empty, it will return Grant that grant `accountId` with `FullControl` permission
func ParseAndValidateAclHeadersOrElseDefault(r *http.Request, accountManager AccountManager, ownership, bucketOwnerId, accountId string, putAcl bool) (ownerId string, grants []*s3.Grant, errCode s3err.ErrorCode) {
	ownerId, grants, errCode = ParseAndValidateAclHeaders(r, accountManager, ownership, bucketOwnerId, accountId, putAcl)
	if errCode != s3err.ErrNone {
		return
	}
	if len(grants) == 0 {
		//if no acl(both customAcl and cannedAcl) specified, grant accountId(object writer) with full control permission
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   &accountId,
			},
			Permission: &s3_constants.PermissionFullControl,
		})
	}
	return
}

// ParseAndValidateAclHeaders parse and validate acl from header
func ParseAndValidateAclHeaders(r *http.Request, accountManager AccountManager, ownership, bucketOwnerId, accountId string, putAcl bool) (ownerId string, grants []*s3.Grant, errCode s3err.ErrorCode) {
	ownerId, grants, errCode = ParseAclHeaders(r, ownership, bucketOwnerId, accountId, putAcl)
	if errCode != s3err.ErrNone {
		return
	}
	if len(grants) > 0 {
		grants, errCode = ValidateAndTransferGrants(accountManager, grants)
	}
	return
}

// ParseAclHeaders parse acl headers
// When `putAcl` is true, only `CannedAcl` is parsed, such as `PutBucketAcl` or `PutObjectAcl`
// is requested, `CustomAcl` is parsed from the request body not from headers, and only if the
// request body is empty, `CannedAcl` is parsed from the header, and will not parse `CustomAcl` from the header
//
// Since `CustomAcl` has higher priority, it will be parsed first; if `CustomAcl` does not exist, `CannedAcl` will be parsed
func ParseAclHeaders(r *http.Request, ownership, bucketOwnerId, accountId string, putAcl bool) (ownerId string, grants []*s3.Grant, errCode s3err.ErrorCode) {
	if !putAcl {
		errCode = ParseCustomAclHeaders(r, &grants)
		if errCode != s3err.ErrNone {
			return "", nil, errCode
		}
	}
	if len(grants) > 0 {
		return accountId, grants, s3err.ErrNone
	}

	cannedAcl := r.Header.Get(s3_constants.AmzCannedAcl)
	if len(cannedAcl) == 0 {
		return accountId, grants, s3err.ErrNone
	}

	//if canned acl specified, parse cannedAcl (lower priority to custom acl)
	ownerId, grants, errCode = ParseCannedAclHeader(ownership, bucketOwnerId, accountId, cannedAcl, putAcl)
	if errCode != s3err.ErrNone {
		return "", nil, errCode
	}
	return ownerId, grants, errCode
}

func ParseCustomAclHeaders(r *http.Request, grants *[]*s3.Grant) s3err.ErrorCode {
	customAclHeaders := []string{s3_constants.AmzAclFullControl, s3_constants.AmzAclRead, s3_constants.AmzAclReadAcp, s3_constants.AmzAclWrite, s3_constants.AmzAclWriteAcp}
	var errCode s3err.ErrorCode
	for _, customAclHeader := range customAclHeaders {
		headerValue := r.Header.Get(customAclHeader)
		switch customAclHeader {
		case s3_constants.AmzAclRead:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionRead, grants)
		case s3_constants.AmzAclWrite:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionWrite, grants)
		case s3_constants.AmzAclReadAcp:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionReadAcp, grants)
		case s3_constants.AmzAclWriteAcp:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionWriteAcp, grants)
		case s3_constants.AmzAclFullControl:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionFullControl, grants)
		}
		if errCode != s3err.ErrNone {
			return errCode
		}
	}
	return s3err.ErrNone
}

func ParseCustomAclHeader(headerValue, permission string, grants *[]*s3.Grant) s3err.ErrorCode {
	if len(headerValue) > 0 {
		split := strings.Split(headerValue, ", ")
		for _, grantStr := range split {
			kv := strings.Split(grantStr, "=")
			if len(kv) != 2 {
				return s3err.ErrInvalidRequest
			}

			switch kv[0] {
			case "id":
				var accountId string
				_ = json.Unmarshal([]byte(kv[1]), &accountId)
				*grants = append(*grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &accountId,
					},
					Permission: &permission,
				})
			case "emailAddress":
				var emailAddress string
				_ = json.Unmarshal([]byte(kv[1]), &emailAddress)
				*grants = append(*grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type:         &s3_constants.GrantTypeAmazonCustomerByEmail,
						EmailAddress: &emailAddress,
					},
					Permission: &permission,
				})
			case "uri":
				var groupName string
				_ = json.Unmarshal([]byte(kv[1]), &groupName)
				*grants = append(*grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeGroup,
						URI:  &groupName,
					},
					Permission: &permission,
				})
			}
		}
	}
	return s3err.ErrNone

}

func ParseCannedAclHeader(bucketOwnership, bucketOwnerId, accountId, cannedAcl string, putAcl bool) (ownerId string, grants []*s3.Grant, err s3err.ErrorCode) {
	err = s3err.ErrNone
	ownerId = accountId

	//objectWrite automatically has full control on current object
	objectWriterFullControl := &s3.Grant{
		Grantee: &s3.Grantee{
			ID:   &accountId,
			Type: &s3_constants.GrantTypeCanonicalUser,
		},
		Permission: &s3_constants.PermissionFullControl,
	}

	switch cannedAcl {
	case s3_constants.CannedAclPrivate:
		grants = append(grants, objectWriterFullControl)
	case s3_constants.CannedAclPublicRead:
		grants = append(grants, objectWriterFullControl)
		grants = append(grants, s3_constants.PublicRead...)
	case s3_constants.CannedAclPublicReadWrite:
		grants = append(grants, objectWriterFullControl)
		grants = append(grants, s3_constants.PublicReadWrite...)
	case s3_constants.CannedAclAuthenticatedRead:
		grants = append(grants, objectWriterFullControl)
		grants = append(grants, s3_constants.AuthenticatedRead...)
	case s3_constants.CannedAclLogDeliveryWrite:
		grants = append(grants, objectWriterFullControl)
		grants = append(grants, s3_constants.LogDeliveryWrite...)
	case s3_constants.CannedAclBucketOwnerRead:
		grants = append(grants, objectWriterFullControl)
		if bucketOwnerId != "" && bucketOwnerId != accountId {
			grants = append(grants,
				&s3.Grant{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &bucketOwnerId,
					},
					Permission: &s3_constants.PermissionRead,
				})
		}
	case s3_constants.CannedAclBucketOwnerFullControl:
		if bucketOwnerId != "" {
			// if set ownership to 'BucketOwnerPreferred' when upload object, the bucket owner will be the object owner
			if !putAcl && bucketOwnership == s3_constants.OwnershipBucketOwnerPreferred {
				ownerId = bucketOwnerId
				grants = append(grants,
					&s3.Grant{
						Grantee: &s3.Grantee{
							Type: &s3_constants.GrantTypeCanonicalUser,
							ID:   &bucketOwnerId,
						},
						Permission: &s3_constants.PermissionFullControl,
					})
			} else {
				grants = append(grants, objectWriterFullControl)
				if accountId != bucketOwnerId {
					grants = append(grants,
						&s3.Grant{
							Grantee: &s3.Grantee{
								Type: &s3_constants.GrantTypeCanonicalUser,
								ID:   &bucketOwnerId,
							},
							Permission: &s3_constants.PermissionFullControl,
						})
				}
			}
		}
	case s3_constants.CannedAclAwsExecRead:
		err = s3err.ErrNotImplemented
	default:
		err = s3err.ErrInvalidRequest
	}
	return
}

// ValidateAndTransferGrants validate grant & transfer Email-Grant to Id-Grant
func ValidateAndTransferGrants(accountManager AccountManager, grants []*s3.Grant) ([]*s3.Grant, s3err.ErrorCode) {
	var result []*s3.Grant
	for _, grant := range grants {
		grantee := grant.Grantee
		if grantee == nil || grantee.Type == nil {
			glog.Warning("invalid grantee! grantee or granteeType is nil")
			return nil, s3err.ErrInvalidRequest
		}

		switch *grantee.Type {
		case s3_constants.GrantTypeGroup:
			if grantee.URI == nil {
				glog.Warning("invalid group grantee! group URI is nil")
				return nil, s3err.ErrInvalidRequest
			}
			ok := s3_constants.ValidateGroup(*grantee.URI)
			if !ok {
				glog.Warningf("invalid group grantee! group name[%s] is not valid", *grantee.URI)
				return nil, s3err.ErrInvalidRequest
			}
			result = append(result, grant)
		case s3_constants.GrantTypeCanonicalUser:
			if grantee.ID == nil {
				glog.Warning("invalid canonical grantee! account id is nil")
				return nil, s3err.ErrInvalidRequest
			}
			name := accountManager.GetAccountNameById(*grantee.ID)
			if len(name) == 0 {
				glog.Warningf("invalid canonical grantee! account id[%s] is not exists", *grantee.ID)
				return nil, s3err.ErrInvalidRequest
			}
			result = append(result, grant)
		case s3_constants.GrantTypeAmazonCustomerByEmail:
			if grantee.EmailAddress == nil {
				glog.Warning("invalid email grantee! email address is nil")
				return nil, s3err.ErrInvalidRequest
			}
			accountId := accountManager.GetAccountIdByEmail(*grantee.EmailAddress)
			if len(accountId) == 0 {
				glog.Warningf("invalid email grantee! email address[%s] is not exists", *grantee.EmailAddress)
				return nil, s3err.ErrInvalidRequest
			}
			result = append(result, &s3.Grant{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeCanonicalUser,
					ID:   &accountId,
				},
				Permission: grant.Permission,
			})
		default:
			return nil, s3err.ErrInvalidRequest
		}
	}
	return result, s3err.ErrNone
}

// GetAcpGrants return grants parsed from entry
func GetAcpGrants(entryExtended map[string][]byte) []*s3.Grant {
	acpBytes, ok := entryExtended[s3_constants.ExtAmzAclKey]
	if ok && len(acpBytes) > 0 {
		var grants []*s3.Grant
		err := json.Unmarshal(acpBytes, &grants)
		if err == nil {
			return grants
		}
	}
	return nil
}

// AssembleEntryWithAcp fill entry with owner and grants
func AssembleEntryWithAcp(objectEntry *filer_pb.Entry, objectOwner string, grants []*s3.Grant) s3err.ErrorCode {
	if objectEntry.Extended == nil {
		objectEntry.Extended = make(map[string][]byte)
	}

	if len(objectOwner) > 0 {
		objectEntry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(objectOwner)
	} else {
		delete(objectEntry.Extended, s3_constants.ExtAmzOwnerKey)
	}

	if len(grants) > 0 {
		grantsBytes, err := json.Marshal(grants)
		if err != nil {
			glog.Warning("assemble acp to entry:", err)
			return s3err.ErrInvalidRequest
		}
		objectEntry.Extended[s3_constants.ExtAmzAclKey] = grantsBytes
	} else {
		delete(objectEntry.Extended, s3_constants.ExtAmzAclKey)
	}

	return s3err.ErrNone
}

