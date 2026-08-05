package s3api

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// A suspended-versioning multipart completion writes the null version at the regular
// path, so it has to drop the null delete marker a preceding DELETE left in .versions.
// Otherwise the completion reports 200 and the object lists, but HEAD/GET keep
// resolving to the delete marker and answer NoSuchKey.
func TestSuspendedMultipartOverwritesDeleteMarker(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)
	suspendVersioning(t, client, bucketName)

	objectKey := "suspended-multipart-after-delete.bin"
	partData := bytes.Repeat([]byte("a"), 5*1024*1024)

	completeSuspendedMultipart := func() {
		t.Helper()
		createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err)

		uploadResp, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   createResp.UploadId,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err)

		_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: createResp.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{{ETag: uploadResp.ETag, PartNumber: aws.Int32(1)}},
			},
		})
		require.NoError(t, err)
	}

	completeSuspendedMultipart()

	_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)

	completeSuspendedMultipart()

	headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	require.NotNil(t, headResp.ContentLength)
	assert.Equal(t, int64(len(partData)), *headResp.ContentLength)

	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()
	written, err := io.Copy(io.Discard, getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, int64(len(partData)), written)

	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Empty(t, listResp.DeleteMarkers)
	require.Len(t, listResp.Versions, 1)
	require.NotNil(t, listResp.Versions[0].VersionId)
	assert.Equal(t, "null", *listResp.Versions[0].VersionId)
}
