package s3api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

// icebergVendedCredentialsProvider names the origin recorded on sessions the
// catalog mints, so audit trails can tell them apart from ordinary logins.
const icebergVendedCredentialsProvider = "iceberg-catalog"

// SetIcebergCredentialRole configures the role the Iceberg catalog assumes when
// a client asks for vended credentials. Vending stays off until it is set.
func (s3a *S3ApiServer) SetIcebergCredentialRole(roleArn string, durationSeconds int64) {
	s3a.icebergCredentialRole = strings.TrimSpace(roleArn)
	s3a.icebergCredentialDuration = durationSeconds
}

// VendedCredentials are short-lived S3 credentials scoped to one table.
type VendedCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

// VendTableCredentials mints credentials that can read and write exactly one
// table's prefix, for a caller the Iceberg catalog has already authenticated
// and authorized. Returns nil when no role is configured, which leaves the
// catalog telling the client to keep using its own credentials.
func (s3a *S3ApiServer) VendTableCredentials(ctx context.Context, principal, bucket, prefix string) (*VendedCredentials, error) {
	if s3a.icebergCredentialRole == "" {
		return nil, nil
	}
	if principal == "" {
		return nil, fmt.Errorf("no principal to vend credentials for")
	}
	if bucket == "" {
		return nil, fmt.Errorf("no bucket to scope credentials to")
	}

	stsService := s3a.stsService()
	if stsService == nil {
		return nil, fmt.Errorf("STS is not configured")
	}

	policy, err := tablePrefixSessionPolicy(bucket, prefix)
	if err != nil {
		return nil, err
	}

	request := &sts.AssumeRoleForPrincipalRequest{
		RoleArn:         s3a.icebergCredentialRole,
		Principal:       principal,
		RoleSessionName: icebergSessionName(principal, bucket, prefix),
		ProviderName:    icebergVendedCredentialsProvider,
		Policy:          &policy,
	}
	if s3a.icebergCredentialDuration > 0 {
		duration := s3a.icebergCredentialDuration
		request.DurationSeconds = &duration
	}

	resp, err := stsService.AssumeRoleForPrincipal(ctx, request)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Credentials == nil {
		return nil, fmt.Errorf("STS returned no credentials")
	}

	return &VendedCredentials{
		AccessKeyID:     resp.Credentials.AccessKeyId,
		SecretAccessKey: resp.Credentials.SecretAccessKey,
		SessionToken:    resp.Credentials.SessionToken,
		Expiration:      resp.Credentials.Expiration,
	}, nil
}

func (s3a *S3ApiServer) stsService() *sts.STSService {
	if s3a.iam == nil || s3a.iam.iamIntegration == nil {
		return nil
	}
	provider, ok := s3a.iam.iamIntegration.(IAMManagerProvider)
	if !ok {
		return nil
	}
	manager := provider.GetIAMManager()
	if manager == nil {
		return nil
	}
	return manager.GetSTSService()
}

// tablePrefixSessionPolicy narrows a session to one table's files plus the
// bucket listing that clients need to resolve them.
func tablePrefixSessionPolicy(bucket, prefix string) (string, error) {
	prefix = strings.Trim(prefix, "/")
	objects := fmt.Sprintf("arn:aws:s3:::%s/*", bucket)
	if prefix != "" {
		objects = fmt.Sprintf("arn:aws:s3:::%s/%s/*", bucket, prefix)
	}

	policy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect":   "Allow",
				"Action":   []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"},
				"Resource": []string{objects},
			},
			{
				"Effect":   "Allow",
				"Action":   []string{"s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetBucketLocation"},
				"Resource": []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)},
			},
		},
	}

	encoded, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("build session policy: %w", err)
	}
	return string(encoded), nil
}

// icebergSessionName builds a session name that identifies the caller and the
// table in audit logs, within the 64-character AWS limit.
func icebergSessionName(principal, bucket, prefix string) string {
	name := fmt.Sprintf("iceberg-%s-%s", principal, strings.ReplaceAll(strings.Trim(prefix, "/"), "/", "-"))
	if strings.Trim(prefix, "/") == "" {
		name = fmt.Sprintf("iceberg-%s-%s", principal, bucket)
	}
	name = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}
