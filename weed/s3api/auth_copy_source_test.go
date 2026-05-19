package s3api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// newCopyRequest builds a request that mimics what a CopyObject caller sends:
// a PUT to the destination bucket/object with X-Amz-Copy-Source pointing at the
// source. The destination is what the Auth middleware would have authorized.
func newCopyRequest(t *testing.T, dstBucket, dstObject, copySource string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, "http://s3.local/"+dstBucket+"/"+dstObject, nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Copy-Source", copySource)
	return req
}

// TestAuthorizeCopySource_AuthDisabled verifies the source check is a no-op when
// the IAM is not enabled (no identities configured).
func TestAuthorizeCopySource_AuthDisabled(t *testing.T) {
	iam := &IdentityAccessManagement{}
	require.False(t, iam.isEnabled())

	req := newCopyRequest(t, "dst-bucket", "dst-obj", "/src-bucket/src-obj")
	errCode := iam.AuthorizeCopySource(req, nil, "src-bucket", "src-obj", "")
	assert.Equal(t, s3err.ErrNone, errCode)
}

// TestAuthorizeCopySource_NilIdentityDenies verifies that with auth enabled we
// fail closed if no identity is available.
func TestAuthorizeCopySource_NilIdentityDenies(t *testing.T) {
	iam := &IdentityAccessManagement{}
	iam.identities = []*Identity{{Name: "anyone"}}
	iam.isAuthEnabled = true
	require.True(t, iam.isEnabled())

	req := newCopyRequest(t, "dst-bucket", "dst-obj", "/src-bucket/src-obj")
	errCode := iam.AuthorizeCopySource(req, nil, "src-bucket", "src-obj", "")
	assert.Equal(t, s3err.ErrAccessDenied, errCode)
}

// TestAuthorizeCopySource_AdminAllowed verifies admin identities bypass the
// source check.
func TestAuthorizeCopySource_AdminAllowed(t *testing.T) {
	iam := &IdentityAccessManagement{}
	iam.identities = []*Identity{{Name: "admin", Actions: []Action{s3_constants.ACTION_ADMIN}}}
	iam.isAuthEnabled = true

	admin := &Identity{Name: "admin", Account: &AccountAdmin, Actions: []Action{s3_constants.ACTION_ADMIN}}
	req := newCopyRequest(t, "dst-bucket", "dst-obj", "/src-bucket/src-obj")
	errCode := iam.AuthorizeCopySource(req, admin, "src-bucket", "src-obj", "")
	assert.Equal(t, s3err.ErrNone, errCode)
}

// TestAuthorizeCopySource_PrefixScopedIdentity checks that an identity scoped
// to prefix-a/* is denied when copying from prefix-b/*, even when its
// destination (prefix-a/*) write permission is fine.
func TestAuthorizeCopySource_PrefixScopedIdentity(t *testing.T) {
	iam := &IdentityAccessManagement{}
	iam.isAuthEnabled = true

	scoped := &Identity{
		Name:    "scoped",
		Account: &AccountAdmin,
		Actions: []Action{
			Action("Read:bucket-a/prefix-a/*"),
			Action("Write:bucket-a/prefix-a/*"),
		},
	}
	iam.identities = []*Identity{scoped}

	t.Run("source within scope is allowed", func(t *testing.T) {
		req := newCopyRequest(t, "bucket-a", "prefix-a/dst.bin", "/bucket-a/prefix-a/src.bin")
		errCode := iam.AuthorizeCopySource(req, scoped, "bucket-a", "prefix-a/src.bin", "")
		assert.Equal(t, s3err.ErrNone, errCode)
	})

	t.Run("source outside scope is denied", func(t *testing.T) {
		req := newCopyRequest(t, "bucket-a", "prefix-a/dst.bin", "/bucket-a/prefix-b/src.bin")
		errCode := iam.AuthorizeCopySource(req, scoped, "bucket-a", "prefix-b/src.bin", "")
		assert.Equal(t, s3err.ErrAccessDenied, errCode)
	})

	t.Run("source in different bucket is denied", func(t *testing.T) {
		req := newCopyRequest(t, "bucket-a", "prefix-a/dst.bin", "/other-bucket/src.bin")
		errCode := iam.AuthorizeCopySource(req, scoped, "other-bucket", "src.bin", "")
		assert.Equal(t, s3err.ErrAccessDenied, errCode)
	})
}

// TestAuthorizeCopySource_IAMIntegrationGetsSourceResource verifies that the
// STS / IAM path is invoked with the SOURCE bucket/object and a GetObject-style
// action — not the destination from the request URL.
func TestAuthorizeCopySource_IAMIntegrationGetsSourceResource(t *testing.T) {
	var capturedAction Action
	var capturedBucket, capturedObject string
	var capturedMethod string
	var capturedURLPath string

	mock := &MockIAMIntegration{
		authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
			// Resolve the action like S3IAMIntegration.AuthorizeAction would,
			// so the assertion reflects the s3:* the policy engine sees.
			capturedAction = Action(ResolveS3Action(r, string(action), bucket, object))
			capturedBucket = bucket
			capturedObject = object
			if r != nil {
				capturedMethod = r.Method
				if r.URL != nil {
					capturedURLPath = r.URL.Path
				}
			}
			// Deny: simulate a session policy that does not grant Read on src.
			return s3err.ErrAccessDenied
		},
	}

	iam := &IdentityAccessManagement{iamIntegration: mock}
	iam.identities = []*Identity{{Name: "sts"}}
	iam.isAuthEnabled = true

	stsIdentity := &Identity{
		Name:         "arn:aws:sts::assumed-role/scopedRole/session",
		Account:      &AccountAdmin,
		Actions:      []Action{}, // STS identity
		PrincipalArn: "arn:aws:sts::assumed-role/scopedRole/session",
	}

	req := newCopyRequest(t, "bucket-a", "prefix-a/dst.bin", "/bucket-a/prefix-b/src.bin")
	req.Header.Set("X-Amz-Security-Token", "test-session-token")

	errCode := iam.AuthorizeCopySource(req, stsIdentity, "bucket-a", "prefix-b/src.bin", "")
	assert.Equal(t, s3err.ErrAccessDenied, errCode)
	assert.True(t, mock.authCalled, "IAM integration must be invoked for STS source check")

	// Action passed through ResolveS3Action: GET method + ACTION_READ -> s3:GetObject.
	assert.Equal(t, "s3:GetObject", string(capturedAction))
	assert.Equal(t, "bucket-a", capturedBucket)
	assert.Equal(t, "prefix-b/src.bin", capturedObject)
	assert.Equal(t, http.MethodGet, capturedMethod, "synthetic source request must be a GET")
	assert.Equal(t, "/bucket-a/prefix-b/src.bin", capturedURLPath, "synthetic URL must target the source")

	// Original request must not be mutated by the source check.
	assert.Equal(t, http.MethodPut, req.Method)
	assert.Equal(t, "/bucket-a/prefix-a/dst.bin", req.URL.Path)
}

// TestAuthorizeCopySource_IAMIntegrationAllow verifies the IAM integration can
// grant access; the source check then returns ErrNone.
func TestAuthorizeCopySource_IAMIntegrationAllow(t *testing.T) {
	mock := &MockIAMIntegration{
		authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
			return s3err.ErrNone
		},
	}

	iam := &IdentityAccessManagement{iamIntegration: mock}
	iam.identities = []*Identity{{Name: "sts"}}
	iam.isAuthEnabled = true

	stsIdentity := &Identity{
		Name:         "arn:aws:sts::assumed-role/role/session",
		Account:      &AccountAdmin,
		Actions:      []Action{},
		PrincipalArn: "arn:aws:sts::assumed-role/role/session",
	}

	req := newCopyRequest(t, "bucket-a", "prefix-a/dst.bin", "/bucket-a/prefix-a/src.bin")
	req.Header.Set("X-Amz-Security-Token", "test-session-token")

	errCode := iam.AuthorizeCopySource(req, stsIdentity, "bucket-a", "prefix-a/src.bin", "")
	assert.Equal(t, s3err.ErrNone, errCode)
}

// TestAuthorizeCopySource_VersionIdPropagated verifies that when copying a
// specific source version, the IAM check sees a versionId on the synthetic
// request so that s3:VersionId conditions and s3:GetObjectVersion resolution
// work.
func TestAuthorizeCopySource_VersionIdPropagated(t *testing.T) {
	var capturedAction Action
	var capturedRawQuery string

	mock := &MockIAMIntegration{
		authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
			capturedAction = Action(ResolveS3Action(r, string(action), bucket, object))
			if r != nil && r.URL != nil {
				capturedRawQuery = r.URL.RawQuery
			}
			return s3err.ErrNone
		},
	}

	iam := &IdentityAccessManagement{iamIntegration: mock}
	iam.isAuthEnabled = true

	stsIdentity := &Identity{
		Name:         "arn:aws:sts::assumed-role/role/session",
		Account:      &AccountAdmin,
		Actions:      []Action{},
		PrincipalArn: "arn:aws:sts::assumed-role/role/session",
	}

	req := newCopyRequest(t, "bucket-a", "dst.bin", "/bucket-a/src.bin?versionId=abc123")
	req.Header.Set("X-Amz-Security-Token", "test-session-token")

	errCode := iam.AuthorizeCopySource(req, stsIdentity, "bucket-a", "src.bin", "abc123")
	assert.Equal(t, s3err.ErrNone, errCode)
	assert.Equal(t, "s3:GetObjectVersion", string(capturedAction), "versioned source should resolve to GetObjectVersion")
	assert.Equal(t, "versionId=abc123", capturedRawQuery)
}

// TestAuthorizeCopySource_PreservesCopySourceHeader verifies that the synthetic
// source request still carries the X-Amz-Copy-Source header so policy condition
// keys like s3:x-amz-copy-source remain available.
func TestAuthorizeCopySource_PreservesCopySourceHeader(t *testing.T) {
	var capturedCopySource string

	mock := &MockIAMIntegration{
		authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
			if r != nil {
				capturedCopySource = r.Header.Get("X-Amz-Copy-Source")
			}
			return s3err.ErrNone
		},
	}

	iam := &IdentityAccessManagement{iamIntegration: mock}
	iam.isAuthEnabled = true

	stsIdentity := &Identity{
		Name:         "arn:aws:sts::assumed-role/role/session",
		Account:      &AccountAdmin,
		Actions:      []Action{},
		PrincipalArn: "arn:aws:sts::assumed-role/role/session",
	}

	req := newCopyRequest(t, "bucket-a", "dst.bin", "/bucket-a/src.bin")
	req.Header.Set("X-Amz-Security-Token", "test-session-token")

	_ = iam.AuthorizeCopySource(req, stsIdentity, "bucket-a", "src.bin", "")
	assert.Equal(t, "/bucket-a/src.bin", capturedCopySource)
}
