package s3api

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuspendedDeleteCreatesDeleteMarker(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)

	objectKey := "suspended-delete-marker.txt"
	versionedResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte("versioned-content")),
	})
	require.NoError(t, err)
	require.NotNil(t, versionedResp.VersionId)

	suspendVersioning(t, client, bucketName)

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte("null-version-content")),
	})
	require.NoError(t, err)

	deleteResp, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp.DeleteMarker)
	assert.True(t, *deleteResp.DeleteMarker)
	require.NotNil(t, deleteResp.VersionId)

	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	require.Len(t, listResp.DeleteMarkers, 1)

	deleteMarker := listResp.DeleteMarkers[0]
	require.NotNil(t, deleteMarker.Key)
	assert.Equal(t, objectKey, *deleteMarker.Key)
	require.NotNil(t, deleteMarker.VersionId)
	assert.Equal(t, *deleteResp.VersionId, *deleteMarker.VersionId)
	require.NotNil(t, deleteMarker.IsLatest)
	assert.True(t, *deleteMarker.IsLatest)

	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.Error(t, err)

	getVersionedResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: versionedResp.VersionId,
	})
	require.NoError(t, err)
	defer getVersionedResp.Body.Close()

	body, err := io.ReadAll(getVersionedResp.Body)
	require.NoError(t, err)
	assert.Equal(t, "versioned-content", string(body))
}
