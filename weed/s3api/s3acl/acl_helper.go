package s3acl

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"net/http"
	"strings"
)

var customAclHeaders = []string{s3_constants.AmzAclFullControl, s3_constants.AmzAclRead, s3_constants.AmzAclReadAcp, s3_constants.AmzAclWrite, s3_constants.AmzAclWriteAcp}

// GetAccountId get AccountId from request headers, AccountAnonymousId will be return if not presen
func GetAccountId(r *http.Request) string {
	id := r.Header.Get(s3_constants.AmzAccountId)
	if len(id) == 0 {
		return s3account.AccountAnonymous.Id
	} else {
		return id
	}
}

// ValidateAccount validate weather request account id is allowed to access
func ValidateAccount(requestAccountId string, allowedAccounts ...string) bool {
	for _, allowedAccount := range allowedAccounts {
		if requestAccountId == allowedAccount {
			return true
		}
	}
	return false
}

// ExtractBucketAcl extracts the acl from the request body, or from the header if request body is empty
func ExtractBucketAcl(r *http.Request, accountManager *s3account.AccountManager, objectOwnership, bucketOwnerId, requestAccountId string, createBucket bool) (grants []*s3.Grant, errCode s3err.ErrorCode) {
	cannedAclPresent := false
	if r.Header.Get(s3_constants.AmzCannedAcl) != "" {
		cannedAclPresent = true
	}
	customAclPresent := false
	for _, customAclHeader := range customAclHeaders {
		if r.Header.Get(customAclHeader) != "" {
			customAclPresent = true
			break
		}
	}

	// AccessControlList body is not support when create object/bucket
	if !createBucket && r.Body != nil && r.Body != http.NoBody {
		defer util.CloseRequest(r)
		if cannedAclPresent || customAclPresent {
			return nil, s3err.ErrUnexpectedContent
		}
		var acp s3.AccessControlPolicy
		err := xmlutil.UnmarshalXML(&acp, xml.NewDecoder(r.Body), "")
		if err != nil || acp.Owner == nil || acp.Owner.ID == nil {
			return nil, s3err.ErrInvalidRequest
		}

		//owner should present && owner is immutable
		if *acp.Owner.ID == "" || *acp.Owner.ID != bucketOwnerId {
			glog.V(3).Infof("set acl denied! owner account is not consistent, request account id: %s, expect account id: %s", *acp.Owner.ID, bucketOwnerId)
			return nil, s3err.ErrAccessDenied
		}
		grants = acp.Grants
	} else {
		if cannedAclPresent && customAclPresent {
			return nil, s3err.ErrInvalidRequest
		}
		if cannedAclPresent {
			grants, errCode = ExtractBucketCannedAcl(r, requestAccountId)
		} else if customAclPresent {
			grants, errCode = ExtractCustomAcl(r)
		}
		if errCode != s3err.ErrNone {
			return nil, errCode
		}
	}
	errCode = ValidateObjectOwnershipAndGrants(objectOwnership, bucketOwnerId, grants)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}
	grants, errCode = ValidateAndTransferGrants(accountManager, grants)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}
	return grants, s3err.ErrNone
}

// ExtractObjectAcl extracts the acl from the request body, or from the header if request body is empty
func ExtractObjectAcl(r *http.Request, accountManager *s3account.AccountManager, objectOwnership, bucketOwnerId, requestAccountId string, createObject bool) (ownerId string, grants []*s3.Grant, errCode s3err.ErrorCode) {
	cannedAclPresent := false
	if r.Header.Get(s3_constants.AmzCannedAcl) != "" {
		cannedAclPresent = true
	}
	customAclPresent := false
	for _, customAclHeader := range customAclHeaders {
		if r.Header.Get(customAclHeader) != "" {
			customAclPresent = true
			break
		}
	}

	// AccessControlList body is not support when create object/bucket
	if !createObject && r.Body != nil && r.Body != http.NoBody {
		defer util.CloseRequest(r)
		if cannedAclPresent || customAclPresent {
			return "", nil, s3err.ErrUnexpectedContent
		}
		var acp s3.AccessControlPolicy
		err := xmlutil.UnmarshalXML(&acp, xml.NewDecoder(r.Body), "")
		if err != nil || acp.Owner == nil || acp.Owner.ID == nil {
			return "", nil, s3err.ErrInvalidRequest
		}

		//owner should present && owner is immutable
		if *acp.Owner.ID == "" {
			glog.V(1).Infof("Access denied! The owner id is required when specifying grants using AccessControlList")
			return "", nil, s3err.ErrAccessDenied
		}
		ownerId = *acp.Owner.ID
		grants = acp.Grants
	} else {
		if cannedAclPresent && customAclPresent {
			return "", nil, s3err.ErrInvalidRequest
		}
		if cannedAclPresent {
			ownerId, grants, errCode = ExtractObjectCannedAcl(r, objectOwnership, bucketOwnerId, requestAccountId, createObject)
		} else {
			grants, errCode = ExtractCustomAcl(r)
		}
		if errCode != s3err.ErrNone {
			return "", nil, errCode
		}
	}
	errCode = ValidateObjectOwnershipAndGrants(objectOwnership, bucketOwnerId, grants)
	if errCode != s3err.ErrNone {
		return "", nil, errCode
	}
	grants, errCode = ValidateAndTransferGrants(accountManager, grants)
	return ownerId, grants, errCode
}

func ExtractCustomAcl(r *http.Request) ([]*s3.Grant, s3err.ErrorCode) {
	var errCode s3err.ErrorCode
	var grants []*s3.Grant
	for _, customAclHeader := range customAclHeaders {
		headerValue := r.Header.Get(customAclHeader)
		switch customAclHeader {
		case s3_constants.AmzAclRead:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionRead, &grants)
		case s3_constants.AmzAclWrite:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionWrite, &grants)
		case s3_constants.AmzAclReadAcp:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionReadAcp, &grants)
		case s3_constants.AmzAclWriteAcp:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionWriteAcp, &grants)
		case s3_constants.AmzAclFullControl:
			errCode = ParseCustomAclHeader(headerValue, s3_constants.PermissionFullControl, &grants)
		default:
			errCode = s3err.ErrInvalidAclArgument
		}
		if errCode != s3err.ErrNone {
			return nil, errCode
		}
	}
	return grants, s3err.ErrNone
}

func ParseCustomAclHeader(headerValue, permission string, grants *[]*s3.Grant) s3err.ErrorCode {
	if len(headerValue) > 0 {
		split := strings.Split(headerValue, ",")
		for _, grantStr := range split {
			kv := strings.Split(grantStr, "=")
			if len(kv) != 2 {
				return s3err.ErrInvalidRequest
			}

			switch strings.TrimSpace(kv[0]) {
			case "id":
				accountId := decodeGranteeValue(kv[1])
				*grants = append(*grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type: &s3_constants.GrantTypeCanonicalUser,
						ID:   &accountId,
					},
					Permission: &permission,
				})
			case "emailAddress":
				emailAddress := decodeGranteeValue(kv[1])
				*grants = append(*grants, &s3.Grant{
					Grantee: &s3.Grantee{
						Type:         &s3_constants.GrantTypeAmazonCustomerByEmail,
						EmailAddress: &emailAddress,
					},
					Permission: &permission,
				})
			case "uri":
				var groupName string
				groupName = decodeGranteeValue(kv[1])
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

func decodeGranteeValue(value string) (result string) {
	if !strings.HasPrefix(value, "\"") {
		return value
	}
	_ = json.Unmarshal([]byte(value), &result)
	if result == "" {
		result = value
	}
	return result
}

// ExtractBucketCannedAcl parse bucket canned acl, includes: 'private'|'public-read'|'public-read-write'|'authenticated-read'
func ExtractBucketCannedAcl(request *http.Request, requestAccountId string) (grants []*s3.Grant, err s3err.ErrorCode) {
	cannedAcl := request.Header.Get(s3_constants.AmzCannedAcl)
	if cannedAcl == "" {
		return grants, s3err.ErrNone
	}
	err = s3err.ErrNone
	objectWriterFullControl := &s3.Grant{
		Grantee: &s3.Grantee{
			ID:   &requestAccountId,
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
	default:
		err = s3err.ErrInvalidAclArgument
	}
	return
}

// ExtractObjectCannedAcl parse object canned acl, includes: 'private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'|'bucket-owner-read'|'bucket-owner-full-control'
func ExtractObjectCannedAcl(request *http.Request, objectOwnership, bucketOwnerId, requestAccountId string, createObject bool) (ownerId string, grants []*s3.Grant, errCode s3err.ErrorCode) {
	if createObject {
		ownerId = requestAccountId
	}

	cannedAcl := request.Header.Get(s3_constants.AmzCannedAcl)
	if cannedAcl == "" {
		return ownerId, grants, s3err.ErrNone
	}

	errCode = s3err.ErrNone
	objectWriterFullControl := &s3.Grant{
		Grantee: &s3.Grantee{
			ID:   &requestAccountId,
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
		if requestAccountId != bucketOwnerId {
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
			if createObject && objectOwnership == s3_constants.OwnershipBucketOwnerPreferred {
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
				if requestAccountId != bucketOwnerId {
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
	default:
		errCode = s3err.ErrInvalidAclArgument
	}
	return
}

// ValidateAndTransferGrants validate grant entity exists and transfer Email-Grant to Id-Grant
func ValidateAndTransferGrants(accountManager *s3account.AccountManager, grants []*s3.Grant) ([]*s3.Grant, s3err.ErrorCode) {
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
			_, ok := accountManager.IdNameMapping[*grantee.ID]
			if !ok {
				glog.Warningf("invalid canonical grantee! account id[%s] is not exists", *grantee.ID)
				return nil, s3err.ErrInvalidRequest
			}
			result = append(result, grant)
		case s3_constants.GrantTypeAmazonCustomerByEmail:
			if grantee.EmailAddress == nil {
				glog.Warning("invalid email grantee! email address is nil")
				return nil, s3err.ErrInvalidRequest
			}
			accountId, ok := accountManager.EmailIdMapping[*grantee.EmailAddress]
			if !ok {
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

// ValidateObjectOwnershipAndGrants validate if grants equals OwnerFullControl when 'ObjectOwnership' is 'BucketOwnerEnforced'
func ValidateObjectOwnershipAndGrants(objectOwnership, bucketOwnerId string, grants []*s3.Grant) s3err.ErrorCode {
	if len(grants) == 0 {
		return s3err.ErrNone
	}
	if objectOwnership == "" {
		objectOwnership = s3_constants.DefaultObjectOwnership
	}
	if objectOwnership != s3_constants.OwnershipBucketOwnerEnforced {
		return s3err.ErrNone
	}
	if len(grants) > 1 {
		return s3err.AccessControlListNotSupported
	}

	bucketOwnerFullControlGrant := &s3.Grant{
		Permission: &s3_constants.PermissionFullControl,
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeCanonicalUser,
			ID:   &bucketOwnerId,
		},
	}
	if GrantEquals(bucketOwnerFullControlGrant, grants[0]) {
		return s3err.ErrNone
	}
	return s3err.AccessControlListNotSupported
}

// DetermineRequiredGrants generates the grant set (Grants) according to accountId and reqPermission.
func DetermineRequiredGrants(accountId, permission string) (grants []*s3.Grant) {
	// group grantee (AllUsers)
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeGroup,
			URI:  &s3_constants.GranteeGroupAllUsers,
		},
		Permission: &permission,
	})
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeGroup,
			URI:  &s3_constants.GranteeGroupAllUsers,
		},
		Permission: &s3_constants.PermissionFullControl,
	})

	// canonical grantee (accountId)
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeCanonicalUser,
			ID:   &accountId,
		},
		Permission: &permission,
	})
	grants = append(grants, &s3.Grant{
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeCanonicalUser,
			ID:   &accountId,
		},
		Permission: &s3_constants.PermissionFullControl,
	})

	// group grantee (AuthenticateUsers)
	if accountId != s3account.AccountAnonymous.Id {
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAuthenticatedUsers,
			},
			Permission: &permission,
		})
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeGroup,
				URI:  &s3_constants.GranteeGroupAuthenticatedUsers,
			},
			Permission: &s3_constants.PermissionFullControl,
		})
	}
	return
}

func SetAcpOwnerHeader(r *http.Request, acpOwnerId string) {
	r.Header.Set(s3_constants.ExtAmzOwnerKey, acpOwnerId)
}

func GetAcpOwner(entryExtended map[string][]byte, defaultOwner string) string {
	ownerIdBytes, ok := entryExtended[s3_constants.ExtAmzOwnerKey]
	if ok && len(ownerIdBytes) > 0 {
		return string(ownerIdBytes)
	}
	return defaultOwner
}

func SetAcpGrantsHeader(r *http.Request, acpGrants []*s3.Grant) {
	if len(acpGrants) > 0 {
		a, err := json.Marshal(acpGrants)
		if err == nil {
			r.Header.Set(s3_constants.ExtAmzAclKey, string(a))
		} else {
			glog.Warning("Marshal acp grants err", err)
		}
	}
}

// GetAcpGrants return grants parsed from entry
func GetAcpGrants(ownerId *string, entryExtended map[string][]byte) []*s3.Grant {
	acpBytes, ok := entryExtended[s3_constants.ExtAmzAclKey]
	if ok && len(acpBytes) > 0 {
		var grants []*s3.Grant
		err := json.Unmarshal(acpBytes, &grants)
		if err == nil {
			return grants
		}
		glog.Warning("grants Unmarshal error", err)
	}
	if ownerId == nil {
		return nil
	}
	return []*s3.Grant{
		{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   ownerId,
			},
			Permission: &s3_constants.PermissionFullControl,
		},
	}
}

// AssembleEntryWithAcp fill entry with owner and grants
func AssembleEntryWithAcp(filerEntry *filer_pb.Entry, ownerId string, grants []*s3.Grant) s3err.ErrorCode {
	if filerEntry.Extended == nil {
		filerEntry.Extended = make(map[string][]byte)
	}

	if len(ownerId) > 0 {
		filerEntry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(ownerId)
	} else {
		delete(filerEntry.Extended, s3_constants.ExtAmzOwnerKey)
	}

	if grants != nil {
		grantsBytes, err := json.Marshal(grants)
		if err != nil {
			glog.Warning("assemble acp to entry:", err)
			return s3err.ErrInvalidRequest
		}
		filerEntry.Extended[s3_constants.ExtAmzAclKey] = grantsBytes
	} else {
		delete(filerEntry.Extended, s3_constants.ExtAmzAclKey)
	}

	return s3err.ErrNone
}

// GrantEquals Compare whether two Grants are equal in meaning, not completely
// equal (compare Grantee.Type and the corresponding Value for equality, other
// fields of Grantee are ignored)
func GrantEquals(a, b *s3.Grant) bool {
	// grant
	if a == b {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	// grant.Permission
	if a.Permission != b.Permission {
		if a.Permission == nil || b.Permission == nil {
			return false
		}

		if *a.Permission != *b.Permission {
			return false
		}
	}

	// grant.Grantee
	ag := a.Grantee
	bg := b.Grantee
	if ag != bg {
		if ag == nil || bg == nil {
			return false
		}
		// grantee.Type
		if ag.Type != bg.Type {
			if ag.Type == nil || bg.Type == nil {
				return false
			}
			if *ag.Type != *bg.Type {
				return false
			}
		}
		// value corresponding to granteeType
		if ag.Type != nil {
			switch *ag.Type {
			case s3_constants.GrantTypeGroup:
				if ag.URI != bg.URI {
					if ag.URI == nil || bg.URI == nil {
						return false
					}

					if *ag.URI != *bg.URI {
						return false
					}
				}
			case s3_constants.GrantTypeCanonicalUser:
				if ag.ID != bg.ID {
					if ag.ID == nil || bg.ID == nil {
						return false
					}

					if *ag.ID != *bg.ID {
						return false
					}
				}
			case s3_constants.GrantTypeAmazonCustomerByEmail:
				if ag.EmailAddress != bg.EmailAddress {
					if ag.EmailAddress == nil || bg.EmailAddress == nil {
						return false
					}

					if *ag.EmailAddress != *bg.EmailAddress {
						return false
					}
				}
			}
		}
	}
	return true
}

func MarshalGrantsToJson(grants []*s3.Grant) ([]byte, error) {
	if len(grants) == 0 {
		return nil, nil
	}
	var GrantsToMap []map[string]any
	for _, grant := range grants {
		grantee := grant.Grantee
		switch *grantee.Type {
		case s3_constants.GrantTypeGroup:
			GrantsToMap = append(GrantsToMap, map[string]any{
				"Permission": grant.Permission,
				"Grantee": map[string]any{
					"Type": grantee.Type,
					"URI":  grantee.URI,
				},
			})
		case s3_constants.GrantTypeCanonicalUser:
			GrantsToMap = append(GrantsToMap, map[string]any{
				"Permission": grant.Permission,
				"Grantee": map[string]any{
					"Type": grantee.Type,
					"ID":   grantee.ID,
				},
			})
		case s3_constants.GrantTypeAmazonCustomerByEmail:
			GrantsToMap = append(GrantsToMap, map[string]any{
				"Permission": grant.Permission,
				"Grantee": map[string]any{
					"Type":         grantee.Type,
					"EmailAddress": grantee.EmailAddress,
				},
			})
		default:
			return nil, fmt.Errorf("grantee type[%s] is not valid", *grantee.Type)
		}
	}

	return json.Marshal(GrantsToMap)
}

func GrantWithFullControl(accountId string) *s3.Grant {
	return &s3.Grant{
		Permission: &s3_constants.PermissionFullControl,
		Grantee: &s3.Grantee{
			Type: &s3_constants.GrantTypeCanonicalUser,
			ID:   &accountId,
		},
	}
}

func CheckObjectAccessForReadObject(r *http.Request, w http.ResponseWriter, entry *filer.Entry, bucketOwnerId string) (statusCode int, ok bool) {
	if entry.IsDirectory() {
		return http.StatusOK, true
	}

	requestAccountId := GetAccountId(r)
	if len(requestAccountId) == 0 {
		glog.Warning("#checkObjectAccessForReadObject header[accountId] not exists!")
		return http.StatusForbidden, false
	}

	//owner access
	objectOwner := GetAcpOwner(entry.Extended, bucketOwnerId)
	if ValidateAccount(requestAccountId, objectOwner) {
		return http.StatusOK, true
	}

	//find in Grants
	acpGrants := GetAcpGrants(nil, entry.Extended)
	if acpGrants != nil {
		reqGrants := DetermineRequiredGrants(requestAccountId, s3_constants.PermissionRead)
		for _, requiredGrant := range reqGrants {
			for _, grant := range acpGrants {
				if GrantEquals(requiredGrant, grant) {
					return http.StatusOK, true
				}
			}
		}
	}

	glog.V(3).Infof("acl denied! request account id: %s", requestAccountId)
	return http.StatusForbidden, false
}
