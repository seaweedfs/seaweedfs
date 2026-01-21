package s3api

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConditionalWritesWithVersioning verifies that conditional writes (If-Match)
// work correctly with versioned buckets, specifically ensuring they validate against
// the LATEST version of the object, not the base object.
// reproduces issue #8073
func TestConditionalWritesWithVersioning(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Enable versioning
	enableVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusEnabled)

	key := "cond-write-test"

	// 1. Create Version 1
	v1Resp := putObject(t, client, bucketName, key, "content-v1")
	require.NotNil(t, v1Resp.ETag)
	require.NotNil(t, v1Resp.VersionId)
	v1ETag := *v1Resp.ETag
	t.Logf("Created Version 1: ETag=%s, VersionId=%s", v1ETag, *v1Resp.VersionId)

	// 2. Create Version 2 (This is now the LATEST version)
	v2Resp := putObject(t, client, bucketName, key, "content-v2")
	require.NotNil(t, v2Resp.ETag)
	require.NotNil(t, v2Resp.VersionId)
	v2ETag := *v2Resp.ETag
	t.Logf("Created Version 2: ETag=%s, VersionId=%s", v2ETag, *v2Resp.VersionId)

	require.NotEqual(t, v1ETag, v2ETag, "ETags should be different for different content")

	// 3. Attempt conditional PUT using Version 1's ETag (If-Match: v1ETag)
	// EXPECTATION: Should FAIL with 412 Precondition Failed because the latest version is V2.
	// BUG (Issue #8073): Previously, this might have succeeded if it checked against an old/stale entry or base entry.
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(key),
		Body:    strings.NewReader("content-v3-should-fail"),
		IfMatch: aws.String(v1ETag),
	})

	if err == nil {
		assert.Fail(t, "Conditional PUT with stale ETag should have failed", "Expected 412 Precondition Failed, but got success")
	} else {
		// Verify strict error checking
		// Check for 412 Precondition Failed
		is412 := false

		// AWS SDK v2 specific error handling
		if strings.Contains(err.Error(), "PreconditionFailed") ||
			strings.Contains(err.Error(), "412") {
			is412 = true
		}

		if !is412 {
			t.Errorf("Expected PreconditionFailed (412) error, but got: %v", err)
		} else {
			t.Logf("Received expected 412 Precondition Failed error: %v", err)
		}
	}

	// 4. Attempt conditional PUT using Version 2's ETag (If-Match: v2ETag)
	// EXPECTATION: Should SUCCEED because V2 is the latest version.
	v4Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(key),
		Body:    strings.NewReader("content-v4-should-succeed"),
		IfMatch: aws.String(v2ETag),
	})
	require.NoError(t, err, "Conditional PUT with correct latest ETag should succeed")
	require.NotNil(t, v4Resp, "PutObject response should not be nil on success")
	require.NotNil(t, v4Resp.ETag, "ETag should not be nil on successful PutObject")
	require.NotNil(t, v4Resp.VersionId, "VersionId should not be nil on successful PutObject")
	t.Logf("Created Version 4: ETag=%s, VersionId=%s", *v4Resp.ETag, *v4Resp.VersionId)

	// 5. Verify the updates
	// The content should be "content-v4-should-succeed"
	headResp := headObject(t, client, bucketName, key)
	require.NotNil(t, headResp.VersionId, "VersionId should not be nil on HeadObject response")
	assert.Equal(t, *v4Resp.VersionId, *headResp.VersionId)

	// Verify actual content
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()
	body, _ := io.ReadAll(getResp.Body)
	assert.Equal(t, "content-v4-should-succeed", string(body), "Content should match the successful conditional write")
}
