package s3api

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Version ids that can never name a stored version. A request carrying one has
// to be refused: resolving it to the null or latest version instead would let a
// caller destroy a live version by asking for one that does not exist.
var unusableVersionIDs = []string{"a/b", `a\b`, "..", "."}

func statusOf(t *testing.T, err error) int {
	t.Helper()
	require.Error(t, err)
	var respErr *smithyhttp.ResponseError
	require.True(t, errors.As(err, &respErr), "expected an HTTP response error, got %v", err)
	return respErr.HTTPStatusCode()
}

func TestUnusableVersionIDIsRefusedNotResolved(t *testing.T) {
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

	for _, versionID := range unusableVersionIDs {
		t.Run("delete "+versionID, func(t *testing.T) {
			_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       aws.String(objectKey),
				VersionId: aws.String(versionID),
			})
			// 400, not merely "some 4xx": the id is malformed, which is a bad
			// argument rather than a missing resource. A 5xx would be the real
			// damage — it invites endless retries of a request that can never
			// succeed — but pinning the exact code keeps the contract honest.
			assert.Equal(t, 400, statusOf(t, err))
		})
	}

	// The point of the whole test: none of those requests may have been resolved
	// to the version that does exist.
	head, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: put.VersionId,
	})
	require.NoError(t, err, "the stored version must survive every refused request")
	require.NotNil(t, head.VersionId)
	assert.Equal(t, *put.VersionId, *head.VersionId)

	get, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "the latest version must still resolve")
	defer get.Body.Close()
}

// Read paths must agree with the write path: the same id is unusable everywhere,
// so a caller cannot be told the object is missing by one verb and served by another.
func TestUnusableVersionIDRefusedOnReadPaths(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	objectKey := "arbitration.lock"
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte("held")),
	})
	require.NoError(t, err)

	for _, versionID := range unusableVersionIDs {
		t.Run("get "+versionID, func(t *testing.T) {
			_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       aws.String(objectKey),
				VersionId: aws.String(versionID),
			})
			assert.Equal(t, 400, statusOf(t, err))
		})
		t.Run("head "+versionID, func(t *testing.T) {
			_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       aws.String(objectKey),
				VersionId: aws.String(versionID),
			})
			assert.Equal(t, 400, statusOf(t, err))
		})
	}
}
