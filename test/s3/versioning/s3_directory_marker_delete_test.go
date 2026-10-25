package s3api

import (
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeletedDirectoryMarkerDisappears covers the rclone directory_markers flow: a key
// created with PutObject on "m2/" is deleted, and stops being a key everywhere. The
// directory that carried it goes with it once nothing is left underneath.
func TestDeletedDirectoryMarkerDisappears(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	putObject(t, client, bucketName, "m2/", "")
	putObject(t, client, bucketName, "m2/f.txt", "hi")
	assert.Equal(t, []string{"m2/", "m2/f.txt"}, listKeys(t, client, bucketName, ""))

	deleteKey(t, client, bucketName, "m2/f.txt")
	deleteKey(t, client, bucketName, "m2/")

	assert.Empty(t, listKeys(t, client, bucketName, ""), "the deleted marker is not a key")
	assert.Empty(t, listPrefixes(t, client, bucketName, ""), "and it names no prefix")

	// The file's own version history is untouched by deleting the directory key.
	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	for _, v := range versions.Versions {
		assert.NotEqual(t, "m2/", *v.Key, "a directory marker is not a versioned object")
	}
	for _, m := range versions.DeleteMarkers {
		assert.NotEqual(t, "m2/", *m.Key, "deleting one writes no delete marker")
	}
	assert.Len(t, versions.Versions, 1, "m2/f.txt keeps its version")
	assert.Len(t, versions.DeleteMarkers, 1, "and its delete marker")

	// Re-creating the marker brings the key back.
	putObject(t, client, bucketName, "m2/", "")
	assert.Equal(t, []string{"m2/"}, listKeys(t, client, bucketName, ""))
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("m2/"),
	})
	require.NoError(t, err)
}

// TestDeletedDirectoryMarkerKeepsItsSubtree pins the boundary: deleting the key "m2/"
// says nothing about the objects under it, which keep listing and keep the prefix.
func TestDeletedDirectoryMarkerKeepsItsSubtree(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	putObject(t, client, bucketName, "m2/", "")
	putObject(t, client, bucketName, "m2/keep.txt", "still here")

	deleteKey(t, client, bucketName, "m2/")

	assert.Equal(t, []string{"m2/keep.txt"}, listKeys(t, client, bucketName, ""), "the marker key is gone")
	assert.Equal(t, []string{"m2/"}, listPrefixes(t, client, bucketName, ""),
		"a live child keeps the prefix")
	assert.Equal(t, []string{"m2/keep.txt"}, listKeys(t, client, bucketName, "m2/"))

	// The surviving object is still readable through the prefix that no longer has a key.
	got, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("m2/keep.txt"),
	})
	require.NoError(t, err)
	require.NoError(t, got.Body.Close())
}

// TestDeletedDirectoryMarkerIsGoneFromReads checks the read surfaces once nothing is
// left under the deleted key: the directory goes with it, so the path is not there.
func TestDeletedDirectoryMarkerIsGoneFromReads(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	putObject(t, client, bucketName, "m2/", "")
	deleteKey(t, client, bucketName, "m2/")

	_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("m2/"),
	})
	require.Error(t, err, "HEAD on a deleted directory marker must not answer 200")
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("m2/"),
	})
	requireAPIError(t, err, "NoSuchKey")
	assert.Empty(t, listKeys(t, client, bucketName, "m2/"))
}

// TestDirectoryMarkerDeleteMatchesUnversioned pins the point of the change: a directory
// marker is deleted the same way whether or not the bucket is versioned.
func TestDirectoryMarkerDeleteMatchesUnversioned(t *testing.T) {
	client := getS3Client(t)

	for _, versioned := range []bool{false, true} {
		bucketName := getNewBucketName()
		createBucket(t, client, bucketName)
		if versioned {
			enableVersioning(t, client, bucketName)
		}

		putObject(t, client, bucketName, "m/", "")
		putObject(t, client, bucketName, "m/child.txt", "x")
		deleteKey(t, client, bucketName, "m/")

		assert.Equal(t, []string{"m/child.txt"}, listKeys(t, client, bucketName, ""),
			"versioned=%v: the marker key is gone, the child stays", versioned)
		assert.Equal(t, []string{"m/"}, listPrefixes(t, client, bucketName, ""),
			"versioned=%v: the prefix survives its live child", versioned)

		deleteBucket(t, client, bucketName)
	}
}

// TestDeleteDirectoryMarkerSparesAPromotedFile guards the sharp edge of storing a key
// on a directory entry: writing under an existing object turns that object's entry into
// a directory while it keeps its data, so "m2/" and "m2" end up on the same entry. They
// are still different keys, and deleting one must not destroy the other.
func TestDeleteDirectoryMarkerSparesAPromotedFile(t *testing.T) {
	client := getS3Client(t)

	for _, versioned := range []bool{false, true} {
		bucketName := getNewBucketName()
		createBucket(t, client, bucketName)

		putObject(t, client, bucketName, "m2", "important data")
		if versioned {
			enableVersioning(t, client, bucketName)
		}
		putObject(t, client, bucketName, "m2/child.txt", "child")

		deleteKey(t, client, bucketName, "m2/")

		got, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("m2"),
		})
		require.NoError(t, err, "versioned=%v: deleting m2/ must not delete m2", versioned)
		body, err := io.ReadAll(got.Body)
		require.NoError(t, err)
		require.NoError(t, got.Body.Close())
		assert.Equal(t, "important data", string(body), "versioned=%v", versioned)

		deleteBucket(t, client, bucketName)
	}
}

func listObjects(t *testing.T, client *s3.Client, bucketName, prefix, delimiter string) *s3.ListObjectsV2Output {
	t.Helper()
	input := &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}
	resp, err := client.ListObjectsV2(context.TODO(), input)
	require.NoError(t, err)
	return resp
}

// listKeys lists without a delimiter, so a directory marker shows as the key it is
// instead of rolling up into its own CommonPrefix.
func listKeys(t *testing.T, client *s3.Client, bucketName, prefix string) []string {
	t.Helper()
	resp := listObjects(t, client, bucketName, prefix, "")
	keys := make([]string, 0, len(resp.Contents))
	for _, c := range resp.Contents {
		keys = append(keys, *c.Key)
	}
	return keys
}

func listPrefixes(t *testing.T, client *s3.Client, bucketName, prefix string) []string {
	t.Helper()
	resp := listObjects(t, client, bucketName, prefix, "/")
	prefixes := make([]string, 0, len(resp.CommonPrefixes))
	for _, p := range resp.CommonPrefixes {
		prefixes = append(prefixes, *p.Prefix)
	}
	return prefixes
}

func deleteKey(t *testing.T, client *s3.Client, bucketName, key string) {
	t.Helper()
	_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
}

func requireAPIError(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, code, apiErr.ErrorCode())
}
