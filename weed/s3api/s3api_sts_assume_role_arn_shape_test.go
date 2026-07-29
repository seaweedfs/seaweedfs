package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An ARN that names a user, not a role, is a bad request: answering
// "not authorized to assume role" sends the caller hunting for permissions.
func TestAssumeRole_RejectsNonRoleArn(t *testing.T) {
	manager := newTestSTSIntegrationManager(t)

	const accessKey, secretKey = "adminkey", "adminsecret"
	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "admin",
			Credentials: []*iam_pb.Credential{{AccessKey: accessKey, SecretKey: secretKey}},
			Actions:     []string{"Admin"},
		}},
	}))
	stsHandlers := NewSTSHandlers(manager.GetSTSService(), iam)

	for _, roleArn := range []string{
		"arn:aws:iam:::user/test-user",              // no account id, as reported
		"arn:aws:iam::123456789012:user/test-user",  // canonical user ARN
		"arn:aws:iam::123456789012:group/test-team", // not a principal at all
		"test-user", // not an ARN
	} {
		t.Run(roleArn, func(t *testing.T) {
			body := url.Values{
				"Action":          {"AssumeRole"},
				"Version":         {"2011-06-15"},
				"RoleArn":         {roleArn},
				"RoleSessionName": {"dev-session"},
			}.Encode()
			req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			require.NoError(t, signRequestV4(req, accessKey, secretKey))

			rec := httptest.NewRecorder()
			stsHandlers.handleAssumeRole(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			var resp STSErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, string(STSErrInvalidParameterValue), resp.Error.Code)
			assert.Contains(t, resp.Error.Message, "is not an IAM role ARN")
		})
	}
}
