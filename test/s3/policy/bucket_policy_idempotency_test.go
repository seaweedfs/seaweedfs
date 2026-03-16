package policy

// Tests for S3 bucket policy JSON round-trip idempotency.
// These validate behavior that IaC tools (Terraform, Ansible) depend on:
// PUT a policy, GET it back, and verify the JSON matches exactly.
// See https://github.com/seaweedfs/seaweedfs/issues/8657

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
)

func newS3ClientForCluster(t *testing.T, cluster *TestCluster) *s3.S3 {
	t.Helper()
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(cluster.s3Endpoint),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("admin", "admin", ""),
	})
	require.NoError(t, err)
	return s3.New(sess)
}

// TestBucketPolicyRoundTrip verifies that GetBucketPolicy returns exactly what
// was submitted via PutBucketPolicy, without adding spurious fields like
// "NotResource": null. This is the core issue from #8657.
func TestBucketPolicyRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	time.Sleep(500 * time.Millisecond)

	s3Client := newS3ClientForCluster(t, cluster)
	bucket := uniqueName("policy-rt")

	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	tests := []struct {
		name   string
		policy map[string]interface{}
	}{
		{
			name: "simple allow without NotResource",
			policy: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []interface{}{
					map[string]interface{}{
						"Sid":       "AllowPublicRead",
						"Effect":    "Allow",
						"Principal": "*",
						"Action":    "s3:GetObject",
						"Resource":  "arn:aws:s3:::" + bucket + "/*",
					},
				},
			},
		},
		{
			name: "multiple actions without NotResource",
			policy: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []interface{}{
					map[string]interface{}{
						"Sid":       "ReadWrite",
						"Effect":    "Allow",
						"Principal": "*",
						"Action":    []interface{}{"s3:GetObject", "s3:PutObject"},
						"Resource":  "arn:aws:s3:::" + bucket + "/*",
					},
				},
			},
		},
		{
			name: "allow with NotResource",
			policy: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []interface{}{
					map[string]interface{}{
						"Sid":         "AllowOutsidePublic",
						"Effect":      "Allow",
						"Principal":   "*",
						"Action":      "s3:GetObject",
						"NotResource": "arn:aws:s3:::" + bucket + "/private/*",
					},
				},
			},
		},
		{
			name: "multiple statements with NotResource",
			policy: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []interface{}{
					map[string]interface{}{
						"Sid":       "AllowRead",
						"Effect":    "Allow",
						"Principal": "*",
						"Action":    "s3:GetObject",
						"Resource":  "arn:aws:s3:::" + bucket + "/*",
					},
					map[string]interface{}{
						"Sid":         "DenyPrivateObjects",
						"Effect":      "Deny",
						"Principal":   "*",
						"Action":      "s3:GetObject",
						"NotResource": "arn:aws:s3:::" + bucket + "/public/*",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policyJSON, err := json.Marshal(tc.policy)
			require.NoError(t, err)

			_, err = s3Client.PutBucketPolicy(&s3.PutBucketPolicyInput{
				Bucket: aws.String(bucket),
				Policy: aws.String(string(policyJSON)),
			})
			require.NoError(t, err)

			getOut, err := s3Client.GetBucketPolicy(&s3.GetBucketPolicyInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err)
			require.NotNil(t, getOut.Policy)

			// The returned policy must not contain fields that were not submitted.
			// This is the exact issue from #8657: NotResource:null was being added.
			returnedJSON := *getOut.Policy
			if !hasKey(tc.policy, "NotResource") {
				require.NotContains(t, returnedJSON, "NotResource",
					"returned policy must not contain NotResource when it was not submitted")
			}

			// Semantic comparison of all submitted fields
			require.JSONEq(t, string(policyJSON), returnedJSON,
				"GET should return semantically identical policy to what was PUT")
		})
	}
}

// TestBucketPolicyIdempotentPut verifies that putting the same policy twice
// does not change the returned value — the behavior IaC tools rely on.
func TestBucketPolicyIdempotentPut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	time.Sleep(500 * time.Millisecond)

	s3Client := newS3ClientForCluster(t, cluster)
	bucket := uniqueName("policy-idem")

	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "AllowRead",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::` + bucket + `/*"
		}]
	}`

	// PUT, then GET, then PUT the returned value, then GET again.
	// Both GETs should return the same result.
	_, err = s3Client.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policyJSON),
	})
	require.NoError(t, err)

	getOut1, err := s3Client.GetBucketPolicy(&s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	// Re-PUT the policy that was returned by GET (what Terraform does on update)
	_, err = s3Client.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: getOut1.Policy,
	})
	require.NoError(t, err)

	getOut2, err := s3Client.GetBucketPolicy(&s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	require.JSONEq(t, *getOut1.Policy, *getOut2.Policy,
		"re-PUTting the GET result must produce identical output (idempotency)")
}

// TestBucketPolicyDeleteAndRecreate verifies clean lifecycle.
func TestBucketPolicyDeleteAndRecreate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	time.Sleep(500 * time.Millisecond)

	s3Client := newS3ClientForCluster(t, cluster)
	bucket := uniqueName("policy-del")

	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::` + bucket + `/*"
		}]
	}`

	// PUT
	_, err = s3Client.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policyJSON),
	})
	require.NoError(t, err)

	// DELETE
	_, err = s3Client.DeleteBucketPolicy(&s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	// GET should fail (no policy)
	_, err = s3Client.GetBucketPolicy(&s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.Error(t, err)

	// Re-PUT same policy
	_, err = s3Client.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policyJSON),
	})
	require.NoError(t, err)

	// GET should succeed and be clean
	getOut, err := s3Client.GetBucketPolicy(&s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.NotContains(t, *getOut.Policy, `"NotResource"`,
		"recreated policy must not contain spurious NotResource")
}

// hasKey checks whether any Statement in the policy map contains the given key.
func hasKey(policy map[string]interface{}, key string) bool {
	stmts, ok := policy["Statement"].([]interface{})
	if !ok {
		return false
	}
	for _, s := range stmts {
		stmt, ok := s.(map[string]interface{})
		if !ok {
			continue
		}
		if _, exists := stmt[key]; exists {
			return true
		}
	}
	return false
}

