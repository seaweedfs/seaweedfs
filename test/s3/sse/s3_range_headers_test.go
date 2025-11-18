package sse_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPlainObjectRangeAndHeadHeaders ensures non-SSE objects advertise correct
// Content-Length and Content-Range information for both HEAD and ranged GETs.
func TestPlainObjectRangeAndHeadHeaders(t *testing.T) {
	ctx := context.Background()

	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"range-plain-")
	require.NoError(t, err, "failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// SeaweedFS S3 auto-chunks uploads at 8MiB (see chunkSize in putToFiler).
	// Using 16MiB ensures at least two chunks without stressing CI resources.
	const chunkSize = 8 * 1024 * 1024
	const objectSize = 2 * chunkSize
	objectKey := "plain-range-validation"
	testData := generateTestData(objectSize)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err, "failed to upload test object")

	t.Run("HeadObject reports accurate Content-Length", func(t *testing.T) {
		resp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "HeadObject request failed")
		assert.Equal(t, int64(objectSize), resp.ContentLength, "Content-Length mismatch on HEAD")
		assert.Equal(t, "bytes", aws.ToString(resp.AcceptRanges), "Accept-Ranges should advertise bytes")
	})

	t.Run("Range request across chunk boundary", func(t *testing.T) {
		// Test range that spans an 8MiB chunk boundary (chunkSize - 1KB to chunkSize + 3KB)
		rangeStart := int64(chunkSize - 1024)
		rangeEnd := rangeStart + 4096 - 1
		rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)

		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Range:  aws.String(rangeHeader),
		})
		require.NoError(t, err, "GetObject range request failed")
		defer resp.Body.Close()

		expectedLen := rangeEnd - rangeStart + 1
		assert.Equal(t, expectedLen, resp.ContentLength, "Content-Length must match requested range size")
		assert.Equal(t,
			fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, objectSize),
			aws.ToString(resp.ContentRange),
			"Content-Range header mismatch")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "failed to read range response body")
		assert.Equal(t, int(expectedLen), len(body), "actual bytes read mismatch")
		assert.Equal(t, testData[rangeStart:rangeEnd+1], body, "range payload mismatch")
	})

	t.Run("Suffix range request", func(t *testing.T) {
		const suffixSize = 2048
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Range:  aws.String(fmt.Sprintf("bytes=-%d", suffixSize)),
		})
		require.NoError(t, err, "GetObject suffix range request failed")
		defer resp.Body.Close()

		expectedStart := int64(objectSize - suffixSize)
		expectedEnd := int64(objectSize - 1)
		expectedLen := expectedEnd - expectedStart + 1

		assert.Equal(t, expectedLen, resp.ContentLength, "suffix Content-Length mismatch")
		assert.Equal(t,
			fmt.Sprintf("bytes %d-%d/%d", expectedStart, expectedEnd, objectSize),
			aws.ToString(resp.ContentRange),
			"suffix Content-Range mismatch")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "failed to read suffix range response body")
		assert.Equal(t, int(expectedLen), len(body), "suffix range byte count mismatch")
		assert.Equal(t, testData[expectedStart:expectedEnd+1], body, "suffix range payload mismatch")
	})
}
