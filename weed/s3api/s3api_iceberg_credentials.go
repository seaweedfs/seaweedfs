package s3api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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

// VendTableCredentials mints credentials that can read exactly one table's
// prefix, and write it too unless readOnly, for a caller the Iceberg catalog
// has already authenticated and authorized. Returns nil when no role is
// configured, which leaves the catalog telling the client to keep using its
// own credentials.
//
// The access key the caller authenticated with (from ctx) is recorded in the
// session: a short hash of it ends the session name, so the audit log's
// requester_arn tells apart the holders of several keys on one identity. When
// the caller is a stored identity holding that key, the key id is also carried
// in the session token, binding the session to it: the session ends when the
// key is deleted or deactivated, or its identity removed or disabled
// (validateSTSSessionToken).
func (s3a *S3ApiServer) VendTableCredentials(ctx context.Context, principal, bucket, prefix string, readOnly bool) (*VendedCredentials, error) {
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

	policy, err := tablePrefixSessionPolicy(bucket, prefix, readOnly)
	if err != nil {
		return nil, err
	}

	accessKey := s3_constants.AccessKeyIdFromContext(ctx)
	request := &sts.AssumeRoleForPrincipalRequest{
		RoleArn:           s3a.icebergCredentialRole,
		Principal:         principal,
		RoleSessionName:   icebergSessionName(principal, bucket, prefix, accessKey),
		ProviderName:      icebergVendedCredentialsProvider,
		Policy:            &policy,
		SourceAccessKeyId: accessKey,
	}
	// Bind the session to the caller's key when the caller authenticated with a
	// stored identity's key — a marker its verifier records, so a caller that
	// signed with an STS session key or OIDC token stays unbound even when its
	// principal names a stored identity, whose session a binding would refuse
	// on every request. The marker survives the key being revoked between
	// authentication and this vend: the session then dies on its first request
	// instead of living unbound until it expires. The binding travels under
	// its own claim — SourceAccessKeyId is audit-only for compatibility with
	// tokens minted before the binding existed.
	if storedKey, storedSecret := s3_constants.StoredCredentialFromContext(ctx); storedKey != "" {
		request.BoundAccessKeyId = storedKey
		request.BoundAccessKeyFingerprint = s3_constants.StoredCredentialFingerprint(s3a.iam.stsSigningKey(), storedKey, storedSecret)
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
	if s3a.iam == nil {
		return nil
	}
	return s3a.iam.stsService()
}

func (iam *IdentityAccessManagement) stsService() *sts.STSService {
	if iam.iamIntegration == nil {
		return nil
	}
	provider, ok := iam.iamIntegration.(IAMManagerProvider)
	if !ok {
		return nil
	}
	manager := provider.GetIAMManager()
	if manager == nil {
		return nil
	}
	return manager.GetSTSService()
}

func (iam *IdentityAccessManagement) stsSigningKey() []byte {
	if s := iam.stsService(); s != nil && s.Config != nil {
		return s.Config.SigningKey
	}
	return nil
}

// tablePrefixSessionPolicy narrows a session to one table's files plus the
// bucket listing that clients need to resolve them. A read-only session drops
// the write verbs: the catalog vends no more authority than the caller holds.
func tablePrefixSessionPolicy(bucket, prefix string, readOnly bool) (string, error) {
	prefix = strings.Trim(prefix, "/")
	// A resource pattern is matched with wildcards, so a location carrying one
	// would widen the session to sibling tables. Refuse rather than escape it:
	// nothing SeaweedFS generates contains these.
	if strings.ContainsAny(bucket, "*?") || strings.ContainsAny(prefix, "*?") {
		return "", fmt.Errorf("refusing to scope credentials to a location with wildcards: s3://%s/%s", bucket, prefix)
	}
	// Without a prefix there is nothing to scope to, and the session would carry
	// read and write over every table in the bucket. A table registered at the
	// bucket root is the only way to get here.
	if prefix == "" {
		return "", fmt.Errorf("refusing to scope credentials to a whole bucket: s3://%s", bucket)
	}

	objectActions := []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"}
	if readOnly {
		objectActions = []string{"s3:GetObject"}
	}

	policy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect":   "Allow",
				"Action":   objectActions,
				"Resource": []string{fmt.Sprintf("arn:aws:s3:::%s/%s/*", bucket, prefix)},
			},
			{
				// Listing is granted on the bucket, so without the condition the
				// session could enumerate every other table's object names.
				"Effect":   "Allow",
				"Action":   []string{"s3:ListBucket"},
				"Resource": []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)},
				"Condition": map[string]interface{}{
					"StringLike": map[string]interface{}{
						"s3:prefix": []string{prefix, prefix + "/*"},
					},
				},
			},
			{
				// GetBucketLocation carries no prefix to condition on, so it sits
				// in its own statement rather than being denied by one.
				// ListBucketMultipartUploads is deliberately absent: Iceberg
				// writers complete and abort by upload id, and granting it either
				// leaks in-flight keys bucket-wide or breaks on the same missing
				// prefix.
				"Effect":   "Allow",
				"Action":   []string{"s3:GetBucketLocation"},
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

// icebergSessionKeyTagLength is the hex length of the access key hash that ends
// a vended session's name: 48 bits, so two keys of one identity collide with
// odds of about 2^-48.
const icebergSessionKeyTagLength = 12

// icebergSessionKeyTag is "-k" and the first icebergSessionKeyTagLength hex
// digits of the SHA-256 of the access key id, or "" without a key.
func icebergSessionKeyTag(accessKey string) string {
	if accessKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(accessKey))
	return "-k" + hex.EncodeToString(sum[:])[:icebergSessionKeyTagLength]
}

// icebergSessionName builds a session name that identifies the caller, the
// table and the caller's access key in audit logs, within the 64-character AWS
// limit. The key tag is never cut: the table part gives way to it.
func icebergSessionName(principal, bucket, prefix, accessKey string) string {
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
	tag := icebergSessionKeyTag(accessKey)
	if limit := 64 - len(tag); len(name) > limit {
		name = name[:limit]
	}
	return name + tag
}
