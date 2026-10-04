package s3api

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// putObjectACLContextKey carries validated ACL metadata to every PutObject write
// path without exposing an internal header that a client could forge.
type putObjectACLContextKey struct{}

type putObjectACLMetadata struct {
	extended map[string][]byte
	canned   string
}

// putObjectACLValue ignores unsigned V2 query ACLs and rejects ambiguity.
// Headers stay untouched because authentication verifies the original request.
func putObjectACLValue(r *http.Request, query url.Values, header string) (string, s3err.ErrorCode) {
	// Preserve SigV2's header-only ACL behavior: arbitrary query parameters
	// are not in its canonical resource and must have no effect on grants.
	switch getRequestAuthType(r) {
	case authTypeSignedV2, authTypePresignedV2:
		query = nil
	}
	var queryValues []string
	queryPresent := false
	for key, values := range query {
		if strings.EqualFold(key, header) {
			queryPresent = true
			queryValues = append(queryValues, values...)
		}
	}
	if queryPresent {
		if len(queryValues) != 1 {
			// V4 sorts duplicate values when signing. Choosing the first value
			// would let reordering change the effective ACL without resigning.
			return "", s3err.ErrInvalidRequest
		}
	}
	values := r.Header.Values(header)
	if header == s3_constants.AmzCannedAcl && len(values) > 1 {
		return "", s3err.ErrInvalidRequest
	}
	value := strings.Join(values, ",")
	if queryPresent {
		if len(values) > 0 && value != queryValues[0] {
			return "", s3err.ErrInvalidRequest
		}
		value = queryValues[0]
	}
	return value, s3err.ErrNone
}

// putObjectACLPolicyRequest exposes effective PUT ACLs to policy conditions only
// after authentication. Other operations keep their original request semantics.
func putObjectACLPolicyRequest(r *http.Request, action Action, bucket, object string) (*http.Request, s3err.ErrorCode) {
	if (action != s3_constants.ACTION_WRITE && action != s3_constants.ACTION_WRITE_ACP) ||
		r.Method != http.MethodPut || object == "" || object == "/" ||
		r.Header.Get("X-Amz-Copy-Source") != "" || r.URL.Query().Has("uploadId") ||
		ResolveS3Action(r, string(s3_constants.ACTION_WRITE), bucket, object) != s3_constants.S3_ACTION_PUT_OBJECT {
		return r, s3err.ErrNone
	}
	policyRequest := r.Clone(r.Context())
	query := parseRequestQuery(r)
	for _, header := range []string{s3_constants.AmzCannedAcl, s3_constants.AmzAclFullControl, s3_constants.AmzAclRead, s3_constants.AmzAclReadAcp, s3_constants.AmzAclWrite, s3_constants.AmzAclWriteAcp} {
		value, code := putObjectACLValue(r, query, header)
		if code != s3err.ErrNone {
			return r, code
		}
		if len(r.Header.Values(header)) == 0 && value != "" {
			policyRequest.Header.Set(header, value)
		}
	}
	return policyRequest, s3err.ErrNone
}

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
		value, code := putObjectACLValue(r, query, header)
		if code != s3err.ErrNone {
			return r, code
		}
		if value != "" {
			custom = true
			aclRequest.Header.Set(header, value)
		}
	}
	canned, code := putObjectACLValue(r, query, s3_constants.AmzCannedAcl)
	if code != s3err.ErrNone {
		return r, code
	}
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
		if identity == nil || identity.Account == nil {
			return r, s3err.ErrAccessDenied
		}
		accountID = identity.Account.Id
	}
	if explicit && !s3a.iam.isEnabled() {
		_, object := s3_constants.GetBucketAndObject(r)
		policyRequest, policyCode := putObjectACLPolicyRequest(r, s3_constants.ACTION_WRITE, bucket, object)
		if policyCode != s3err.ErrNone {
			return r, policyCode
		}
		for _, action := range []Action{s3_constants.ACTION_WRITE, s3_constants.ACTION_WRITE_ACP} {
			if policyCode, _ := s3a.checkPolicyWithEntry(policyRequest, bucket, object, string(action), "", nil); policyCode != s3err.ErrNone {
				return r, policyCode
			}
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
		if metadata.Ownership == s3_constants.OwnershipBucketOwnerEnforced {
			// Keep legacy buckets without recorded ownership controls accepting
			// ACLs; only an explicitly configured enforced control disables them.
			if custom || (canned != "" && canned != s3_constants.CannedAclBucketOwnerFullControl) {
				return r, s3err.ErrAccessControlListNotSupported
			}
			aclRequest.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclPrivate)
		}
		accountID = bucketOwner
	}
	if aclRequest.Header.Get(s3_constants.AmzCannedAcl) == "" && !custom {
		aclRequest.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclPrivate)
	}
	// Canned grants contain only authenticated writer and recorded bucket-owner
	// IDs. Dynamic IAM/JWT accounts need not exist in the static account directory.
	// Client-supplied custom grantees must still pass directory validation.
	owner, grants, code := ParseAclHeaders(aclRequest, ownership, bucketOwner, accountID, false)
	if code == s3err.ErrNone && custom {
		grants, code = ValidateAndTransferGrants(s3a.iam, grants)
	}
	if code != s3err.ErrNone {
		return r, code
	}
	entry := &filer_pb.Entry{}
	if code = AssembleEntryWithAcp(entry, owner, grants); code != s3err.ErrNone {
		return r, code
	}
	prepared := putObjectACLMetadata{extended: entry.Extended, canned: canned}
	return r.WithContext(context.WithValue(r.Context(), putObjectACLContextKey{}, prepared)), s3err.ErrNone
}

// applyPutObjectACL adds prevalidated ownership and grants before CreateEntry.
// Multipart parts and POST form uploads do not carry this PutObject context.
func applyPutObjectACL(r *http.Request, entry *filer_pb.Entry) {
	metadata, _ := r.Context().Value(putObjectACLContextKey{}).(putObjectACLMetadata)
	for key, value := range metadata.extended {
		entry.Extended[key] = value
	}
}
