package s3api

import (
	"context"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// putObjectACLContextKey carries validated ACL metadata to every PutObject write
// path without exposing an internal header that a client could forge.
type putObjectACLContextKey struct{}

// preparePutObjectACL validates and authorizes ACLs before the upload body is
// consumed. The resulting metadata is committed in the same entry as the object.
func (s3a *S3ApiServer) preparePutObjectACL(r *http.Request, bucket string) (*http.Request, s3err.ErrorCode) {
	metadata, code := s3a.getBucketConfig(bucket)
	if code != s3err.ErrNone {
		return r, code
	}
	if metadata == nil || s3a.iam == nil {
		return r, s3err.ErrInternalError
	}

	// Presigners can hoist ACL headers into the signed query string. Normalize a
	// separate request for parsing, preserving the original for signature checks.
	aclRequest := r.Clone(r.Context())
	query := parseRequestQuery(r)
	custom := false
	for _, header := range []string{s3_constants.AmzAclFullControl, s3_constants.AmzAclRead, s3_constants.AmzAclReadAcp, s3_constants.AmzAclWrite, s3_constants.AmzAclWriteAcp} {
		value := lookupHeaderOrQuery(r, query, header)
		if value != "" {
			custom = true
			aclRequest.Header.Set(header, value)
		}
	}
	canned := lookupHeaderOrQuery(r, query, s3_constants.AmzCannedAcl)
	aclRequest.Header.Set(s3_constants.AmzCannedAcl, canned)
	explicit := canned != "" || custom
	accountID := r.Header.Get(s3_constants.AmzAccountId)
	if !s3a.iam.isEnabled() {
		accountID = AccountAdmin.Id
	} else if explicit {
		// Setting an ACL during PutObject also requires s3:PutObjectAcl. Use the
		// unified authorization path so bucket-policy allows and explicit denies
		// retain the same semantics as standalone ACL requests.
		identity, authCode := s3a.iam.authRequest(r.Clone(r.Context()), s3_constants.ACTION_WRITE_ACP)
		if authCode != s3err.ErrNone {
			return r, authCode
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

	bucketOwner := metadata.Owner
	if bucketOwner == "" {
		// Buckets created outside S3 can have no recorded owner, matching the
		// bucket registry's existing admin fallback for these entries.
		bucketOwner = AccountAdmin.Id
	}
	ownership := s3_constants.EffectiveOwnership(metadata.Ownership)
	if ownership == s3_constants.OwnershipBucketOwnerEnforced {
		if custom || (canned != "" && canned != s3_constants.CannedAclBucketOwnerFullControl) {
			return r, s3err.ErrAccessControlListNotSupported
		}
		// ACLs are disabled: the bucket owner is the only effective grantee,
		// including when bucket-owner-full-control accompanies the upload.
		accountID = bucketOwner
		aclRequest.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclPrivate)
	}
	owner, grants, code := ParseAndValidateAclHeadersOrElseDefault(aclRequest, s3a.iam, ownership, bucketOwner, accountID, false)
	if code != s3err.ErrNone {
		return r, code
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
