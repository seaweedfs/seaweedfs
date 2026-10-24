package s3api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireAPIErrorCode(t *testing.T, err error, expected string) {
	t.Helper()
	require.Error(t, err)
	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr), "expected a smithy.APIError, got %T: %v", err, err)
	assert.Equal(t, expected, apiErr.ErrorCode())
}

// TestConditionalReadsOfMissingObject verifies that a missing key stays a missing key
// under If-Match and If-Unmodified-Since instead of surfacing as 412.
// reproduces issue #10984
func TestConditionalReadsOfMissingObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	existing := putObject(t, client, bucketName, "etag-source", "content")
	require.NotNil(t, existing.ETag)
	future := aws.Time(time.Now().Add(24 * time.Hour))
	missing := aws.String("conditional-missing")

	t.Run("HeadObject If-Match", func(t *testing.T) {
		_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName), Key: missing, IfMatch: existing.ETag,
		})
		requireAPIErrorCode(t, err, "NotFound")
	})

	t.Run("HeadObject If-Unmodified-Since", func(t *testing.T) {
		_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName), Key: missing, IfUnmodifiedSince: future,
		})
		requireAPIErrorCode(t, err, "NotFound")
	})

	t.Run("GetObject If-Match", func(t *testing.T) {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName), Key: missing, IfMatch: existing.ETag,
		})
		requireAPIErrorCode(t, err, "NoSuchKey")
	})

	t.Run("GetObject If-Unmodified-Since", func(t *testing.T) {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName), Key: missing, IfUnmodifiedSince: future,
		})
		requireAPIErrorCode(t, err, "NoSuchKey")
	})

	t.Run("GetObject stale If-Match on a live object stays 412", func(t *testing.T) {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String("etag-source"),
			IfMatch: aws.String(`"0000000000000000000000000000dead"`),
		})
		requireAPIErrorCode(t, err, "PreconditionFailed")
	})
}

// TestConditionalReadsOfNamedVersion verifies that a conditional GET or HEAD of an
// explicit versionId is evaluated against that version rather than the latest one,
// including when the latest version is a delete marker.
func TestConditionalReadsOfNamedVersion(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	key := "conditional-read-version"
	v1 := putObject(t, client, bucketName, key, "content-v1")
	require.NotNil(t, v1.ETag)
	require.NotNil(t, v1.VersionId)
	v2 := putObject(t, client, bucketName, key, "content-v2")
	require.NotNil(t, v2.ETag)
	require.NotEqual(t, *v1.ETag, *v2.ETag)

	t.Run("If-Match matches the named version, not the latest", func(t *testing.T) {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(key),
			VersionId: v1.VersionId, IfMatch: v1.ETag,
		})
		require.NoError(t, err)
	})

	t.Run("If-Match against the latest ETag fails on the named version", func(t *testing.T) {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(key),
			VersionId: v1.VersionId, IfMatch: v2.ETag,
		})
		requireAPIErrorCode(t, err, "PreconditionFailed")
	})

	_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String(key),
	})
	require.NoError(t, err)

	t.Run("named version survives a delete marker on the latest", func(t *testing.T) {
		_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(key),
			VersionId: v1.VersionId, IfMatch: v1.ETag,
		})
		require.NoError(t, err)
	})

	t.Run("delete marker latest is a missing object", func(t *testing.T) {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(key), IfMatch: v1.ETag,
		})
		requireAPIErrorCode(t, err, "NoSuchKey")
	})
}
