package s3api

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type anonymousObjectReadContextKey struct{}

// isAnonymousObjectRead identifies requests deferred by AuthWithPublicRead to
// the selected entry's authorization check. Clients cannot set this marker.
func isAnonymousObjectRead(r *http.Request) bool {
	marked, _ := r.Context().Value(anonymousObjectReadContextKey{}).(bool)
	return marked
}

// deferAnonymousObjectRead only grants entry-dependent authorization to raw
// GET/HEAD reads. Subresources, writes and requests carrying credentials keep
// their normal authentication path; no-auth development mode is unchanged.
func (s3a *S3ApiServer) deferAnonymousObjectRead(r *http.Request, action Action, bucket, object string) (*http.Request, bool) {
	if s3a.iam == nil || !s3a.iam.isEnabled() || action != s3_constants.ACTION_READ ||
		getRequestAuthType(r) != authTypeAnonymous || object == "" || object == "/" ||
		(r.Method != http.MethodGet && r.Method != http.MethodHead) || isSOSAPIObject(strings.TrimPrefix(object, "/")) {
		return r, false
	}
	resolved := ResolveS3Action(r, string(action), bucket, object)
	if r.URL.Query().Has("uploads") || (resolved != s3_constants.S3_ACTION_GET_OBJECT && resolved != s3_constants.S3_ACTION_GET_OBJECT_VERSION) {
		return r, false
	}
	// A session token without a signature names no session; drop it so it
	// cannot influence identity or policy evaluation downstream.
	r.Header.Del("X-Amz-Security-Token")
	if q := r.URL.Query(); q.Has("X-Amz-Security-Token") {
		q.Del("X-Amz-Security-Token")
		r.URL.RawQuery = q.Encode()
	}

	// Reuse authentication's internal-header sanitization. A public ACL does
	// not require an anonymous identity; a configured one still contributes
	// its permissions and explicit identity-policy denies.
	identity, _, _ := s3a.iam.authenticateRequestInternal(r)
	ctx := recordIdentityInContext(r, identity)
	return r.WithContext(context.WithValue(ctx, anonymousObjectReadContextKey{}, true)), true
}

// authorizeAnonymousObjectRead evaluates policies and ACLs against the same
// entry used for the response, including conditional and versioned reads.
// Explicit policy denies take precedence over every public-access grant.
func (s3a *S3ApiServer) authorizeAnonymousObjectRead(r *http.Request, bucket, object string, extended map[string][]byte) s3err.ErrorCode {
	// A version's physical filer path is not another S3 key: accepting it would
	// bypass GetObjectVersion policies on the logical key. Ordinary user keys
	// containing ".versions" without internal version metadata are unaffected.
	key := s3_constants.NormalizeObjectKey(object)
	version := string(extended[s3_constants.ExtVersionIdKey])
	if version != "" && strings.HasSuffix(path.Dir(key), s3_constants.VersionsFolder) && path.Base(key) == s3a.getVersionFileName(version) {
		return s3err.ErrAccessDenied
	}
	if string(extended[s3_constants.ExtDeleteMarkerKey]) == "true" {
		return s3err.ErrAccessDenied
	}
	// Loading config also synchronizes a cold bucket policy into the engine.
	config, code := s3a.getBucketConfig(bucket)
	if code != s3err.ErrNone {
		return code
	}
	identity, _ := s3_constants.GetIdentityFromContext(r).(*Identity)
	code, policyAllowed := s3a.checkPolicyWithEntry(r, bucket, object, string(s3_constants.ACTION_READ), buildPrincipalARN(identity, r), extended)
	if code != s3err.ErrNone {
		return code
	}
	if s3a.iam.isActionExplicitlyDeniedByApplicablePolicies(r, identity, s3_constants.ACTION_READ, bucket, object) {
		return s3err.ErrAccessDenied
	}
	if policyAllowed {
		return s3err.ErrNone
	}
	if identity != nil && s3a.iam.VerifyActionPermission(r, identity, s3_constants.ACTION_READ, bucket, object) == s3err.ErrNone {
		return s3err.ErrNone
	}
	// Match the upload/ACL path: legacy buckets without recorded ownership
	// controls still accept ACLs. An enforced control explicitly disables them.
	if config.Ownership == "" || s3_constants.EffectiveOwnership(config.Ownership) != s3_constants.OwnershipBucketOwnerEnforced {
		if data, exists := extended[s3_constants.ExtAmzAclKey]; exists {
			// An explicitly stored ACL replaces the legacy bucket-ACL fallback.
			// Empty, malformed and non-public ACLs must not grant anonymous read.
			var grants []*s3.Grant
			if json.Unmarshal(data, &grants) == nil {
				for _, grant := range grants {
					if grant != nil && grant.Grantee != nil && grant.Grantee.Type != nil && *grant.Grantee.Type == "Group" &&
						grant.Grantee.URI != nil && *grant.Grantee.URI == s3_constants.GranteeGroupAllUsers && grant.Permission != nil &&
						(*grant.Permission == s3_constants.PermissionRead || *grant.Permission == s3_constants.PermissionFullControl) {
						return s3err.ErrNone
					}
				}
			}
			return s3err.ErrAccessDenied
		}
	}
	// Preserve SeaweedFS's bucket-public-read behavior for legacy objects that
	// have no stored object ACL; BucketOwnerEnforced ignores object grants.
	if config.IsPublicRead {
		return s3err.ErrNone
	}
	return s3err.ErrAccessDenied
}
