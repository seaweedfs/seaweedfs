package s3api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNativeFloorTestIAM(t *testing.T) (*IdentityAccessManagement, *Identity) {
	t.Helper()
	mgr := newTestIAMManager(t)
	iam := &IdentityAccessManagement{}
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	doc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"},
		},
	})
	require.NoError(t, iam.PutPolicy("ListBucketsOnly", string(doc)))

	identity := &Identity{
		Name:         "admin",
		Account:      &Account{DisplayName: "admin", Id: "admin"},
		Actions:      []Action{s3_constants.ACTION_ADMIN},
		PolicyNames:  []string{"ListBucketsOnly"},
		PrincipalArn: "arn:aws:iam::111122223333:user/admin",
	}
	return iam, identity
}

func putObjectRequest() *http.Request {
	return httptest.NewRequest(http.MethodPut, "/mybucket/file.txt", nil)
}

// TestNativeAdminSurvivesAttachedPolicy reproduces issue #11226: attaching an
// IAM policy to a user with native Admin must not override the native grant.
// The effective permissions are the union of native permissions and attached
// policy grants, so a Write the attached policy never mentions still succeeds.
func TestNativeAdminSurvivesAttachedPolicy(t *testing.T) {
	iam, identity := newNativeFloorTestIAM(t)

	errCode := iam.VerifyActionPermission(putObjectRequest(), identity,
		s3_constants.ACTION_WRITE, "mybucket", "file.txt")
	assert.Equal(t, s3err.ErrNone, errCode,
		"native Admin must remain effective after attaching a policy")
}

// TestNativeAdminSurvivesDeletedPolicy reproduces the second half of #11226:
// deleting the attached policy without detaching it first must not leave the
// user locked out of their native permissions.
func TestNativeAdminSurvivesDeletedPolicy(t *testing.T) {
	iam, identity := newNativeFloorTestIAM(t)

	require.NoError(t, iam.DeletePolicy("ListBucketsOnly"))

	errCode := iam.VerifyActionPermission(putObjectRequest(), identity,
		s3_constants.ACTION_WRITE, "mybucket", "file.txt")
	assert.Equal(t, s3err.ErrNone, errCode,
		"native Admin must remain effective after the attached policy is deleted")
}

// TestNativeAdminSurvivesAttachedPolicyWithoutPrincipalArn covers the IAM
// integration path when the Admin identity has no PrincipalArn (and no session
// token), so the auth-path switch would otherwise deny before the native floor.
func TestNativeAdminSurvivesAttachedPolicyWithoutPrincipalArn(t *testing.T) {
	mgr := newTestIAMManager(t)
	iam := &IdentityAccessManagement{}
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	doc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"},
		},
	})
	require.NoError(t, iam.PutPolicy("ListBucketsOnly", string(doc)))

	identity := &Identity{
		Name:        "admin",
		Account:     &Account{DisplayName: "admin", Id: "admin"},
		Actions:     []Action{s3_constants.ACTION_ADMIN},
		PolicyNames: []string{"ListBucketsOnly"},
	}

	errCode := iam.VerifyActionPermission(putObjectRequest(), identity,
		s3_constants.ACTION_WRITE, "mybucket", "file.txt")
	assert.Equal(t, s3err.ErrNone, errCode,
		"native Admin must remain effective without a PrincipalArn")
}

// TestAttachedPolicyExplicitDenyOverridesNativeAdmin ensures deny-always-wins:
// an explicit Deny in an attached policy still constrains a native admin.
func TestAttachedPolicyExplicitDenyOverridesNativeAdmin(t *testing.T) {
	mgr := newTestIAMManager(t)
	iam := &IdentityAccessManagement{}
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	doc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Deny", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::mybucket/*"},
		},
	})
	require.NoError(t, iam.PutPolicy("DenyPutMyBucket", string(doc)))

	identity := &Identity{
		Name:         "admin",
		Account:      &Account{DisplayName: "admin", Id: "admin"},
		Actions:      []Action{s3_constants.ACTION_ADMIN},
		PolicyNames:  []string{"DenyPutMyBucket"},
		PrincipalArn: "arn:aws:iam::111122223333:user/admin",
	}

	errCode := iam.VerifyActionPermission(putObjectRequest(), identity,
		s3_constants.ACTION_WRITE, "mybucket", "file.txt")
	assert.Equal(t, s3err.ErrAccessDenied, errCode,
		"explicit Deny in an attached policy must override native Admin")
}

// GET ?uploads lists multipart uploads at bucket level but routes under Read,
// so the prefix promoted into object must not change the evaluated action or
// resource: an explicit Deny on s3:ListBucketMultipartUploads must still
// constrain a native admin, and an Allow on the bucket ARN must satisfy a
// plain attached-policy identity.
func TestMultipartListingResolvesBucketAction(t *testing.T) {
	mgr := newTestIAMManager(t)
	iam := &IdentityAccessManagement{}
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	denyDoc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Deny", "Action": "s3:ListBucketMultipartUploads", "Resource": "arn:aws:s3:::mybucket"},
		},
	})
	require.NoError(t, iam.PutPolicy("DenyUploadsListing", string(denyDoc)))
	allowDoc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Allow", "Action": "s3:ListBucketMultipartUploads", "Resource": "arn:aws:s3:::mybucket"},
		},
	})
	require.NoError(t, iam.PutPolicy("AllowUploadsListing", string(allowDoc)))

	uploadsReq := func() *http.Request {
		return httptest.NewRequest(http.MethodGet, "/mybucket?uploads&prefix=x/", nil)
	}
	admin := &Identity{
		Name:         "admin",
		Account:      &Account{DisplayName: "admin", Id: "admin"},
		Actions:      []Action{s3_constants.ACTION_ADMIN},
		PolicyNames:  []string{"DenyUploadsListing"},
		PrincipalArn: "arn:aws:iam::111122223333:user/admin",
	}
	reader := &Identity{
		Name:         "reader",
		Account:      &Account{DisplayName: "reader", Id: "reader"},
		PolicyNames:  []string{"AllowUploadsListing"},
		PrincipalArn: "arn:aws:iam::111122223333:user/reader",
	}

	// object carries the promoted prefix, matching authRequestWithAuthType
	assert.Equal(t, s3err.ErrAccessDenied,
		iam.VerifyActionPermission(uploadsReq(), admin, s3_constants.ACTION_READ, "mybucket", "x/"),
		"explicit Deny on the uploads listing must override native Admin")
	assert.Equal(t, s3err.ErrNone,
		iam.VerifyActionPermission(uploadsReq(), reader, s3_constants.ACTION_READ, "mybucket", "x/"),
		"s3:ListBucketMultipartUploads on the bucket must allow the uploads listing")
}
