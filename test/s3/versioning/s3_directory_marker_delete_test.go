package s3api

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeletedDirectoryMarkerDisappears covers the rclone directory_markers flow: a key
// created with PutObject on "m2/" is deleted, and every current-version surface has to
// agree it is gone even though the filer directory that carries it survives.
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
	assert.Empty(t, listKeys(t, client, bucketName, "m2/"), "prefix=m2/ answers empty")

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

	// The key appears once in the version listing, as its delete marker.
	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("m2/"),
	})
	require.NoError(t, err)
	latest := 0
	for _, v := range versions.Versions {
		if *v.Key == "m2/" && *v.IsLatest {
			latest++
		}
	}
	for _, m := range versions.DeleteMarkers {
		if *m.Key == "m2/" && *m.IsLatest {
			latest++
		}
	}
	assert.Equal(t, 1, latest, "exactly one version of m2/ is the latest")

	// Re-creating the marker retires the delete marker and keeps the history.
	putObject(t, client, bucketName, "m2/", "")
	assert.Equal(t, []string{"m2/"}, listKeys(t, client, bucketName, ""))
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("m2/"),
	})
	require.NoError(t, err)

	versions, err = client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("m2/"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, versions.DeleteMarkers, "the delete marker stays in the history")
	for _, m := range versions.DeleteMarkers {
		if *m.Key == "m2/" {
			assert.False(t, *m.IsLatest, "the delete marker is no longer current")
		}
	}
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

	assert.Equal(t, []string{"m2/keep.txt"}, listKeys(t, client, bucketName, ""))
	assert.Equal(t, []string{"m2/"}, listPrefixes(t, client, bucketName, ""),
		"a live child keeps the prefix even though the marker key is gone")
	assert.Equal(t, []string{"m2/keep.txt"}, listKeys(t, client, bucketName, "m2/"))
}

// TestDirectoryMarkerSurvivesWithoutVersioning guards the unversioned path, which has no
// history to consult and must keep answering as before.
func TestDirectoryMarkerSurvivesWithoutVersioning(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "m2/", "")
	assert.Equal(t, []string{"m2/"}, listKeys(t, client, bucketName, ""))

	_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("m2/"),
	})
	require.NoError(t, err)
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
