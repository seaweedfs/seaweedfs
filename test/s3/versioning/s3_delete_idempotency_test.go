package s3api

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Backup clients probe and retract lock keys constantly, so they delete keys and
// versions that may already be gone. S3 makes those deletes succeed; answering an
// error instead turns routine lock arbitration into a job failure. Object lock is
// enabled here because the retention check runs before the delete and is the most
// likely place for a missing object to be reported as an error.

func TestDeleteMissingObjectSucceeds(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// A key that never existed.
	resp, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("never-written.lock"),
	})
	require.NoError(t, err, "deleting a key that never existed must succeed")
	require.NotNil(t, resp.DeleteMarker, "a versioned bucket records the delete as a marker")
	assert.True(t, *resp.DeleteMarker)
	require.NotNil(t, resp.VersionId)

	// The response can claim a marker the backend never stored, so confirm it is
	// really in the version history under the id the response handed back.
	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("never-written.lock"),
	})
	require.NoError(t, err)
	found := false
	for _, m := range versions.DeleteMarkers {
		if m.Key != nil && *m.Key == "never-written.lock" && m.VersionId != nil && *m.VersionId == *resp.VersionId {
			found = true
		}
	}
	assert.True(t, found, "the delete marker reported by the response must be persisted")
}

func TestDeleteAbsentVersionSucceeds(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	objectKey := "arbitration.lock"
	put, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte("held")),
	})
	require.NoError(t, err)
	require.NotNil(t, put.VersionId)

	// Well-formed version id that names no stored version. Deleting it is a no-op,
	// not an error, and must not disturb the version that does exist.
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String("deadbeefdeadbeefdeadbeefdeadbeef"),
	})
	require.NoError(t, err, "deleting a version that does not exist must succeed")

	head, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: put.VersionId,
	})
	require.NoError(t, err, "the real version must survive the no-op delete")
	require.NotNil(t, head.VersionId)
	assert.Equal(t, *put.VersionId, *head.VersionId)

	// Repeating the delete of the real version is also a no-op rather than an error.
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: put.VersionId,
	})
	require.NoError(t, err)

	// Confirm the first delete actually removed the version. Without this a
	// backend that ignored every version-specific delete would satisfy both calls
	// and the idempotency the test claims to cover would never be exercised.
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: put.VersionId,
	})
	require.Error(t, err, "the version must be gone after the first delete")

	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: put.VersionId,
	})
	require.NoError(t, err, "repeating a version delete must stay a no-op")
}

// A missing key in the middle of a batch must not consume another key's result
// slot: every request key has to come back under its own name.
func TestMultiObjectDeleteAttributesMissingKeys(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	present := []string{"batch-1.lock", "batch-3.lock"}
	for _, key := range present {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("held")),
		})
		require.NoError(t, err)
	}

	requested := []string{"batch-1.lock", "batch-2-missing.lock", "batch-3.lock"}
	objects := make([]types.ObjectIdentifier, 0, len(requested))
	for _, key := range requested {
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
	}

	resp, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{Objects: objects},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Errors, "a missing key is not a batch error")
	require.Len(t, resp.Deleted, len(requested))

	got := make(map[string]bool, len(resp.Deleted))
	for _, d := range resp.Deleted {
		require.NotNil(t, d.Key, "every result row must carry the key it belongs to")
		assert.NotEmpty(t, *d.Key, "an empty key means a row lost its identity")
		got[*d.Key] = true
	}
	for _, key := range requested {
		assert.True(t, got[key], "no result row for %s", key)
	}
}
