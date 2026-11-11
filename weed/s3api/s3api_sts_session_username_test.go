package s3api

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const sessionUserAccessKey, sessionUserSecretKey = "AKIABOB000000000000", "bobSecret123"

func newSharedRoleIAM(t *testing.T) (*integration.IAMManager, *IdentityAccessManagement) {
	t.Helper()
	manager := newTestSTSIntegrationManager(t)
	require.NoError(t, manager.CreateRole(context.Background(), "", "SharedRole", &integration.RoleDefinition{
		RoleName: "SharedRole",
		TrustPolicy: &policy.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
		},
	}))
	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "bob",
			Credentials: []*iam_pb.Credential{{AccessKey: sessionUserAccessKey, SecretKey: sessionUserSecretKey}},
			Actions:     []string{"Read:home"},
		}},
	}))
	return manager, iam
}

func assumeSharedRole(t *testing.T, manager *integration.IAMManager, iam *IdentityAccessManagement, accessKey, secretKey, sessionToken, sessionName string) AssumeRoleResult {
	t.Helper()
	body := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/SharedRole"},
		"RoleSessionName": {sessionName},
	}.Encode()
	req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", sessionToken)
	}
	require.NoError(t, signRequestV4(req, accessKey, secretKey))

	rec := httptest.NewRecorder()
	NewSTSHandlers(manager.GetSTSService(), iam).handleAssumeRole(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	return resp.Result
}

// A role session's ARN ends in the session name its caller chose, so a caller
// that names its session after another user must not get that user's
// ${aws:username}.
func TestAssumeRole_SessionNameIsNotTheUsername(t *testing.T) {
	ctx := context.Background()
	manager, iam := newSharedRoleIAM(t)
	iam.policyEngine = NewBucketPolicyEngine()
	require.NoError(t, iam.policyEngine.engine.SetBucketPolicy("home", `{"Version":"2012-10-17","Statement":[
	  {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::home/${aws:username}/*"},
	  {"Effect":"Deny","Principal":"*","Action":"s3:GetObject","NotResource":"arn:aws:s3:::home/${aws:username}/*"}]}`))

	result := assumeSharedRole(t, manager, iam, sessionUserAccessKey, sessionUserSecretKey, "", "alice")
	creds := result.Credentials
	assert.Equal(t, "arn:aws:sts::"+defaultAccountID+":assumed-role/SharedRole/alice", result.AssumedRoleUser.Arn)

	session, err := manager.GetSTSService().ValidateSessionToken(ctx, creds.SessionToken)
	require.NoError(t, err)
	assert.Equal(t, "bob", session.ExternalUserId, "the session records its caller")

	get := func(key string) s3err.ErrorCode {
		t.Helper()
		r, err := newTestRequest(http.MethodGet, "http://s3.seaweedfs.test/home/"+key, 0, nil)
		require.NoError(t, err)
		r.Header.Set("X-Amz-Security-Token", creds.SessionToken)
		require.NoError(t, signRequestV4(r, creds.AccessKeyId, creds.SecretAccessKey))
		r = mux.SetURLVars(r, map[string]string{"bucket": "home", "object": key})
		_, errCode := iam.authRequest(r, s3_constants.ACTION_READ)
		return errCode
	}
	assert.Equal(t, s3err.ErrAccessDenied, get("alice/notes.txt"), "the session name must not select another user's folder")
	assert.Equal(t, s3err.ErrNone, get("bob/notes.txt"), "the caller keeps its own folder")
}

// A role session that assumes a role passes on its own user, not its session
// ID or the name it picks for the new session.
func TestAssumeRole_ChainedSessionKeepsTheUser(t *testing.T) {
	manager, iam := newSharedRoleIAM(t)
	first := assumeSharedRole(t, manager, iam, sessionUserAccessKey, sessionUserSecretKey, "", "bob").Credentials
	chained := assumeSharedRole(t, manager, iam, first.AccessKeyId, first.SecretAccessKey, first.SessionToken, "alice").Credentials

	session, err := manager.GetSTSService().ValidateSessionToken(context.Background(), chained.SessionToken)
	require.NoError(t, err)
	assert.Equal(t, "bob", session.ExternalUserId)
}
