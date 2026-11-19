package s3api

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSTSSessionPolicyEvaluatesRequestConditions(t *testing.T) {
	ctx := context.Background()
	iamManager := setupGroupsRolesIAMManager(t)
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")

	require.NoError(t, iamManager.CreatePolicy(ctx, "", "RequestConditionPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Effect:   "Allow",
				Action:   []string{"s3:PutObject"},
				Resource: []string{"arn:aws:s3:::cond-bucket/*", "arn:aws:s3:::kms-bucket/*", "arn:aws:s3:::tls-bucket/*"},
			},
			{
				Effect:   "Deny",
				Action:   []string{"s3:PutObject"},
				Resource: []string{"arn:aws:s3:::cond-bucket/*"},
				Condition: map[string]map[string]interface{}{
					"StringEquals": {"s3:x-amz-object-lock-mode": []string{"GOVERNANCE", "COMPLIANCE"}},
				},
			},
			{
				Effect:   "Deny",
				Action:   []string{"s3:PutObject"},
				Resource: []string{"arn:aws:s3:::cond-bucket/*"},
				Condition: map[string]map[string]interface{}{
					"StringEquals": {"s3:RequestObjectTag/classification": "secret"},
				},
			},
			{
				Effect:   "Deny",
				Action:   []string{"s3:PutObject"},
				Resource: []string{"arn:aws:s3:::kms-bucket/*"},
				Condition: map[string]map[string]interface{}{
					"StringNotEquals": {"s3:x-amz-server-side-encryption": "aws:kms"},
				},
			},
			{
				Effect:   "Deny",
				Action:   []string{"s3:PutObject"},
				Resource: []string{"arn:aws:s3:::tls-bucket/*"},
				Condition: map[string]map[string]interface{}{
					"Bool": {"aws:SecureTransport": "false"},
				},
			},
			{
				Effect:   "Deny",
				Action:   []string{"s3:PutObject"},
				Resource: []string{"arn:aws:s3:::cond-bucket/*"},
				Condition: map[string]map[string]interface{}{
					"StringEquals": {"s3:authType": "REST-QUERY-STRING"},
				},
			},
		},
	}))
	require.NoError(t, iamManager.CreateRole(ctx, "", "RequestConditionRole", &integration.RoleDefinition{
		RoleName: "RequestConditionRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{{
				Effect:    "Allow",
				Principal: map[string]interface{}{"Federated": "test-oidc"},
				Action:    []string{"sts:AssumeRoleWithWebIdentity"},
			}},
		},
		AttachedPolicies: []string{"RequestConditionPolicy"},
	}))
	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/RequestConditionRole",
		WebIdentityToken: groupsRolesJWT(t, "condition-user", nil, nil),
		RoleSessionName:  "condition-session",
	})
	require.NoError(t, err)
	identity, errCode := authenticateSessionToken(t, s3iam, response.Credentials.SessionToken)
	require.Equal(t, s3err.ErrNone, errCode)

	tests := []struct {
		name    string
		bucket  string
		query   string
		headers map[string]string
		secure  bool
		want    s3err.ErrorCode
	}{
		{name: "plain write", bucket: "cond-bucket", want: s3err.ErrNone},
		{name: "object lock mode", bucket: "cond-bucket", headers: map[string]string{"X-Amz-Object-Lock-Mode": "GOVERNANCE"}, want: s3err.ErrAccessDenied},
		{name: "presigned v4", bucket: "cond-bucket", query: "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F20261119%2Fus-east-1%2Fs3%2Faws4_request", want: s3err.ErrAccessDenied},
		{name: "presigned v2", bucket: "cond-bucket", query: "?AWSAccessKeyId=key&Expires=1&Signature=sig", want: s3err.ErrAccessDenied},
		{name: "request object tag", bucket: "cond-bucket", headers: map[string]string{"X-Amz-Tagging": "classification=secret"}, want: s3err.ErrAccessDenied},
		{name: "other request object tag", bucket: "cond-bucket", headers: map[string]string{"X-Amz-Tagging": "classification=public"}, want: s3err.ErrNone},
		{name: "kms encryption", bucket: "kms-bucket", headers: map[string]string{"X-Amz-Server-Side-Encryption": "aws:kms"}, want: s3err.ErrNone},
		{name: "kms encryption any case", bucket: "kms-bucket", headers: map[string]string{"X-Amz-Server-Side-Encryption": "AWS:KMS"}, want: s3err.ErrNone},
		{name: "other encryption", bucket: "kms-bucket", headers: map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}, want: s3err.ErrAccessDenied},
		{name: "no encryption", bucket: "kms-bucket", want: s3err.ErrAccessDenied},
		{name: "insecure transport", bucket: "tls-bucket", want: s3err.ErrAccessDenied},
		{name: "secure transport", bucket: "tls-bucket", secure: true, want: s3err.ErrNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/file.txt"+tt.query, http.NoBody)
			if tt.query == "" {
				req.Header.Set("Authorization", "Bearer "+identity.SessionToken)
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			if tt.secure {
				req.TLS = &tls.ConnectionState{}
			}
			assert.Equal(t, tt.want, s3iam.AuthorizeAction(ctx, identity, s3_constants.ACTION_WRITE, tt.bucket, "file.txt", req))
		})
	}
}
