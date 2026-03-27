package s3api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func versioningCopySource(bucketName, key string) string {
	return fmt.Sprintf("%s/%s", bucketName, url.PathEscape(key))
}

func suspendVersioning(t *testing.T, client *s3.Client, bucketName string) {
	_, err := client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusSuspended,
		},
	})
	require.NoError(t, err)
}

func TestVersioningSelfCopyMetadataReplaceCreatesNewVersion(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusEnabled)

	objectKey := "self-copy-versioned.txt"
	initialContent := []byte("copy me without changing the body")

	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		Body:     bytes.NewReader(initialContent),
		Metadata: map[string]string{"stage": "one"},
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.VersionId)

	copyResp, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(objectKey),
		CopySource:        aws.String(versioningCopySource(bucketName, objectKey)),
		Metadata:          map[string]string{"stage": "two"},
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	require.NoError(t, err, "Self-copy with metadata replacement should succeed")
	require.NotNil(t, copyResp.VersionId, "Versioned self-copy should create a new version")
	require.NotEqual(t, *putResp.VersionId, *copyResp.VersionId, "Self-copy should create a distinct version")

	headLatestResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Equal(t, "two", headLatestResp.Metadata["stage"], "Latest version should expose replaced metadata")

	headOriginalResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: putResp.VersionId,
	})
	require.NoError(t, err)
	assert.Equal(t, "one", headOriginalResp.Metadata["stage"], "Previous version metadata should remain intact")

	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, initialContent, body, "Self-copy should not alter the object body")

	versionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versionsResp.Versions, 2, "Self-copy should append a new current version")
	assert.Equal(t, *copyResp.VersionId, *versionsResp.Versions[0].VersionId, "New copy version should be latest")
}

func TestVersioningSelfCopyMetadataReplaceSuspendedKeepsNullVersion(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)
	suspendVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusSuspended)

	objectKey := "self-copy-suspended.txt"
	initialContent := []byte("null version content")

	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		Body:     bytes.NewReader(initialContent),
		Metadata: map[string]string{"stage": "one"},
	})
	require.NoError(t, err)

	copyResp, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(objectKey),
		CopySource:        aws.String(versioningCopySource(bucketName, objectKey)),
		Metadata:          map[string]string{"stage": "two"},
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	require.NoError(t, err, "Suspended self-copy with metadata replacement should succeed")
	assert.Nil(t, copyResp.VersionId, "Suspended versioning should not return a version header for the current null version")

	headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Equal(t, "two", headResp.Metadata["stage"], "Null current version should be updated in place")

	versionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versionsResp.Versions, 1, "Suspended self-copy should keep a single null current version")
	require.NotNil(t, versionsResp.Versions[0].VersionId)
	assert.Equal(t, "null", *versionsResp.Versions[0].VersionId, "Suspended self-copy should preserve null-version semantics")
	assert.True(t, *versionsResp.Versions[0].IsLatest, "Null version should remain latest")
}
