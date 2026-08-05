package s3api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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

func TestVersioningSelfCopyCreatesNewVersion(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)

	objectKey := "self-copy-no-directive.txt"
	firstContent := []byte("first")
	secondContent := []byte("second")

	firstPut, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(firstContent),
	})
	require.NoError(t, err)
	require.NotNil(t, firstPut.VersionId)

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(secondContent),
	})
	require.NoError(t, err)

	// Copying an earlier version onto its own key is how AWS restores that version.
	copyResp, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s?versionId=%s", versioningCopySource(bucketName, objectKey), *firstPut.VersionId)),
	})
	require.NoError(t, err, "Self-copy of an earlier version should succeed on a versioned bucket")
	require.NotNil(t, copyResp.VersionId)
	assert.NotEqual(t, *firstPut.VersionId, *copyResp.VersionId, "Restore should write a new version")

	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, firstContent, body, "Restored version should serve the earlier body")

	versionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Len(t, versionsResp.Versions, 3, "Restore should append to the version history")
}

func TestSelfCopyWithoutVersioningIsRejected(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	objectKey := "self-copy-unversioned.txt"
	putObject(t, client, bucketName, objectKey, "content")

	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		CopySource: aws.String(versioningCopySource(bucketName, objectKey)),
	})
	require.Error(t, err, "Self-copy without a metadata change is a no-op on an unversioned bucket")
	var apiErr smithy.APIError
	if assert.True(t, errors.As(err, &apiErr), "Expected a smithy.APIError, but got %T", err) {
		assert.Equal(t, "InvalidRequest", apiErr.ErrorCode())
	}
}

func TestSelfCopyWithSuspendedVersioningIsRejected(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)
	suspendVersioning(t, client, bucketName)

	objectKey := "self-copy-suspended-no-directive.txt"
	putObject(t, client, bucketName, objectKey, "content")

	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		CopySource: aws.String(versioningCopySource(bucketName, objectKey)),
	})
	require.Error(t, err, "Suspended versioning overwrites the null version in place, so the copy changes nothing")
	var apiErr smithy.APIError
	if assert.True(t, errors.As(err, &apiErr), "Expected a smithy.APIError, but got %T", err) {
		assert.Equal(t, "InvalidRequest", apiErr.ErrorCode())
	}
}

// A suspended-versioning CopyObject writes the null version at the regular path, so
// like PutObject and multipart completion it has to retire the null delete marker a
// preceding DELETE left in .versions. While the regular-path object owns the null
// slot the leftover marker is shadowed, but it resurfaces as a phantom delete the
// moment that null version goes away.
func TestSuspendedCopyRetiresDeleteMarker(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	sourceKey := "suspended-copy-source.txt"
	objectKey := "suspended-copy-dest.txt"

	enableVersioning(t, client, bucketName)
	putObject(t, client, bucketName, objectKey, "pre-suspension-content")
	suspendVersioning(t, client, bucketName)

	putObject(t, client, bucketName, sourceKey, "source-content")
	putObject(t, client, bucketName, objectKey, "null-version-content")
	deleteKey(t, client, bucketName, objectKey)

	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		CopySource: aws.String(versioningCopySource(bucketName, sourceKey)),
	})
	require.NoError(t, err)

	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, "source-content", string(body))

	// Drop the null version the copy just wrote; a retired marker leaves nothing behind.
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String("null"),
	})
	require.NoError(t, err)

	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Empty(t, listResp.DeleteMarkers, "the copy should have retired the null delete marker")
}
