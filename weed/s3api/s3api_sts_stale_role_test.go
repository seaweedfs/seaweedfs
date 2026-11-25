package s3api

import (
	"context"
	"net/http"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// A bucket policy naming a role session grants it only while the session's
// role exists: once the role is deleted, or replaced by a role of the same
// name, the Allow must not keep the old session working.
func TestBucketPolicyAllowEndsWithTheSessionsRole(t *testing.T) {
	ctx := context.Background()
	manager, iam := newSharedRoleIAM(t)
	iam.policyEngine = NewBucketPolicyEngine()

	result := assumeSharedRole(t, manager, iam, sessionUserAccessKey, sessionUserSecretKey, "", "alice")
	creds := result.Credentials
	require.NoError(t, iam.policyEngine.engine.SetBucketPolicy("home", `{"Version":"2012-10-17","Statement":[
	  {"Effect":"Allow","Principal":{"AWS":"`+result.AssumedRoleUser.Arn+`"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::home/*"}]}`))

	get := func() s3err.ErrorCode {
		t.Helper()
		r, err := newTestRequest(http.MethodGet, "http://s3.seaweedfs.test/home/notes.txt", 0, nil)
		require.NoError(t, err)
		r.Header.Set("X-Amz-Security-Token", creds.SessionToken)
		require.NoError(t, signRequestV4(r, creds.AccessKeyId, creds.SecretAccessKey))
		r = mux.SetURLVars(r, map[string]string{"bucket": "home", "object": "notes.txt"})
		_, errCode := iam.authRequest(r, s3_constants.ACTION_READ)
		return errCode
	}
	require.Equal(t, s3err.ErrNone, get(), "precondition: the bucket policy grants the session")

	original, err := manager.GetRole(ctx, "SharedRole")
	require.NoError(t, err)
	require.NoError(t, manager.DeleteRole(ctx, "SharedRole"))
	assert.Equal(t, s3err.ErrAccessDenied, get(), "the session outlived its role's deletion")

	recreated := *original
	recreated.RoleId = ""
	require.NoError(t, manager.CreateRole(ctx, "", "SharedRole", &recreated))
	assert.Equal(t, s3err.ErrAccessDenied, get(), "a session of the deleted role works again under a new role of the same name")
}
