package s3api

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// putObjectACLContextKey carries validated ACL metadata to the created entry
// without exposing an internal header that a client could forge.
type putObjectACLContextKey struct{}

// preparePutObjectACL validates and authorizes ACLs before the upload body is
// consumed, so a rejected ACL neither allocates chunks nor replaces an object.
func (s3a *S3ApiServer) preparePutObjectACL(r *http.Request, bucket string) (*http.Request, s3err.ErrorCode) {
	bucketConfig, code := s3a.getBucketConfig(bucket)
	if code != s3err.ErrNone {
		return r, code
	}
	if bucketConfig == nil || s3a.iam == nil {
		return r, s3err.ErrInternalError
	}

	// Presigners can hoist ACL headers into the signed query string. SigV2 does
	// not sign arbitrary query parameters, so it accepts only headers.
	query := parseRequestQuery(r)
	if r.Header.Get(s3_constants.AmzAuthType) == "SigV2" {
		query = nil
	}
	canned := lookupHeaderOrQuery(r, query, s3_constants.AmzCannedAcl)
	custom := false
	customValues := map[string]string{}
	for _, header := range []string{s3_constants.AmzAclFullControl, s3_constants.AmzAclRead, s3_constants.AmzAclReadAcp, s3_constants.AmzAclWrite, s3_constants.AmzAclWriteAcp} {
		if value := lookupHeaderOrQuery(r, query, header); value != "" {
			custom = true
			customValues[header] = value
		}
	}
	explicit := canned != "" || custom

	accountID := r.Header.Get(s3_constants.AmzAccountId)
	if !s3a.iam.isEnabled() {
		accountID = AccountAdmin.Id
	} else if explicit {
		// Setting an ACL during PutObject also requires s3:PutObjectAcl. This
		// re-verifies the signature, so request headers must stay untouched.
		identity, authCode := s3a.iam.authRequest(r.Clone(r.Context()), s3_constants.ACTION_WRITE_ACP)
		if authCode != s3err.ErrNone {
			return r, authCode
		}
		if identity == nil || identity.Account == nil {
			return r, s3err.ErrAccessDenied
		}
		accountID = identity.Account.Id
	}
	if explicit {
		_, object := s3_constants.GetBucketAndObject(r)
		if policyCode, _ := s3a.checkPolicyWithEntry(r, bucket, object, string(s3_constants.ACTION_WRITE_ACP), "", nil); policyCode != s3err.ErrNone {
			return r, policyCode
		}
	}
	if accountID == "" {
		return r, s3err.ErrAccessDenied
	}
	if canned != "" && custom {
		return r, s3err.ErrInvalidRequest
	}

	bucketOwner := bucketConfig.Owner
	if bucketOwner == "" {
		bucketOwner = AccountAdmin.Id
	}
	ownership := s3_constants.EffectiveOwnership(bucketConfig.Ownership)
	if ownership == s3_constants.OwnershipBucketOwnerEnforced {
		if bucketConfig.Ownership == s3_constants.OwnershipBucketOwnerEnforced {
			// Buckets without an ownership control keep accepting ACLs; only an
			// explicit BucketOwnerEnforced control disables them.
			if custom || (canned != "" && canned != s3_constants.CannedAclBucketOwnerFullControl) {
				return r, s3err.ErrAccessControlListNotSupported
			}
			canned = s3_constants.CannedAclPrivate
		}
		accountID = bucketOwner
	}

	// ACL headers may have arrived in the signed query; mirroring them into the
	// headers keeps grant parsing and resolveFileMode consistent. This is safe
	// only after signature verification above.
	for header, value := range customValues {
		r.Header.Set(header, value)
	}
	if canned != "" {
		r.Header.Set(s3_constants.AmzCannedAcl, canned)
	}

	owner, grants, code := ParseAclHeaders(r, ownership, bucketOwner, accountID, false)
	if code != s3err.ErrNone {
		return r, code
	}
	if custom {
		// Only caller-supplied grantees need registry validation; canned and
		// default grants are built from trusted account ids, which JWT or
		// otherwise external accounts may not appear in.
		grants, code = ValidateAndTransferGrants(s3a.iam, grants)
		if code != s3err.ErrNone {
			return r, code
		}
	}
	if len(grants) == 0 {
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type: &s3_constants.GrantTypeCanonicalUser,
				ID:   &owner,
			},
			Permission: &s3_constants.PermissionFullControl,
		})
	}
	entry := &filer_pb.Entry{}
	if code = AssembleEntryWithAcp(entry, owner, grants); code != s3err.ErrNone {
		return r, code
	}
	return r.WithContext(context.WithValue(r.Context(), putObjectACLContextKey{}, entry.Extended)), s3err.ErrNone
}

// applyPutObjectACL adds prevalidated ownership and grants before CreateEntry.
// Multipart parts and POST form uploads do not carry this PutObject context.
func applyPutObjectACL(r *http.Request, entry *filer_pb.Entry) {
	metadata, _ := r.Context().Value(putObjectACLContextKey{}).(map[string][]byte)
	for key, value := range metadata {
		entry.Extended[key] = value
	}
}
