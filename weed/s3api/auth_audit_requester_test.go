package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
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
