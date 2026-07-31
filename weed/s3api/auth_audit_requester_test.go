package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// An STS session authenticates as an opaque session subject, so the audit entry
// must also carry the principal ARN — the role name and the role session name
// are only recoverable from there.
func TestAuditRequesterArnForSTSSession(t *testing.T) {
	iam := &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{
			validateSessionFunc: func(ctx context.Context, token string) (*sts.SessionInfo, error) {
				return &sts.SessionInfo{
					AssumedRoleUser: "ClientRole/dev-session",
					Principal:       "arn:aws:sts::000000000000:assumed-role/ClientRole/dev-session",
					Subject:         "47ad4828c45b3f337bc3146081ba8f0f",
					SessionName:     "dev-session",
					Credentials: &sts.Credentials{
						AccessKeyId:     "ASIA0189777d42cba8e2",
						SecretAccessKey: "secret",
					},
					ExpiresAt: time.Now().Add(time.Hour),
					Policies:  []string{"ClientPolicy"},
				}, nil
			},
		},
	}

	// track() installs the holder before authentication runs.
	outer := s3_constants.EnsureIdentityHolder(httptest.NewRequest(http.MethodGet, "http://s3/test/", nil))

	identity, _, errCode := iam.validateSTSSessionToken(outer, "session-token", "ASIA0189777d42cba8e2")
	require.Equal(t, s3err.ErrNone, errCode)

	iam.handleAuthResult(httptest.NewRecorder(), outer, identity, s3err.ErrNone, func(http.ResponseWriter, *http.Request) {})

	log := s3err.GetAccessLog(outer, http.StatusOK, s3err.ErrNone)
	assert.Equal(t, "47ad4828c45b3f337bc3146081ba8f0f", log.Requester)
	assert.Equal(t, "arn:aws:sts::000000000000:assumed-role/ClientRole/dev-session", log.RequesterArn,
		"audit entry must name the assumed role and session")
}

// The AssumeRole call itself is authenticated inside the STS handler, which the
// generic auth middleware never wraps; without recording the caller there the
// audit entry for minting a session has no requester at all.
func TestAuditRequesterForAssumeRole(t *testing.T) {
	ctx := context.Background()
	manager := newTestSTSIntegrationManager(t)

	require.NoError(t, manager.CreatePolicy(ctx, "", "ClientPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:*"},
			Resource: []string{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
		}},
	}))
	require.NoError(t, manager.CreateRole(ctx, "", "ClientRole", &integration.RoleDefinition{
		RoleName: "ClientRole",
		TrustPolicy: &policy.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
		},
		AttachedPolicies: []string{"ClientPolicy"},
	}))

	const accessKey, secretKey = "adminkey", "adminsecret"
	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "admin",
			Credentials: []*iam_pb.Credential{{AccessKey: accessKey, SecretKey: secretKey}},
			Actions:     []string{"Admin"},
		}},
	}))

	body := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/ClientRole"},
		"RoleSessionName": {"dev-session"},
	}.Encode()
	req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, signRequestV4(req, accessKey, secretKey))
	req = s3_constants.EnsureIdentityHolder(req)

	rec := httptest.NewRecorder()
	NewSTSHandlers(manager.GetSTSService(), iam).handleAssumeRole(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	log := s3err.GetAccessLog(req, rec.Code, s3err.ErrNone)
	assert.Equal(t, "admin", log.Requester, "AssumeRole must audit who asked for the session")
	assert.Equal(t, "arn:aws:iam::"+defaultAccountID+":user/admin", log.RequesterArn)
}
