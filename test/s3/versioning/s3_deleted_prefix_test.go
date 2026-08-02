package s3api

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeletedPrefixLeavesNoCommonPrefix covers the versioned bucket case where the only
// object under a prefix is deleted. The delete marker and the version history stay, so
// the directory survives in the filer, but a current-version listing has no key under
// that path and AWS reports no CommonPrefix for it.
func TestDeletedPrefixLeavesNoCommonPrefix(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	putObject(t, client, bucketName, "backup/20260101/manifest", "first backup")
	putObject(t, client, bucketName, "backup/20260102/manifest", "second backup")

	listPrefixes := func(prefix string) []string {
		t.Helper()
		input := &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		}
		if prefix != "" {
			input.Prefix = aws.String(prefix)
		}
		resp, err := client.ListObjectsV2(context.TODO(), input)
		require.NoError(t, err)
		prefixes := make([]string, 0, len(resp.CommonPrefixes))
		for _, p := range resp.CommonPrefixes {
			prefixes = append(prefixes, *p.Prefix)
		}
		return prefixes
	}
	listKeys := func(prefix string) []string {
		t.Helper()
		input := &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		}
		if prefix != "" {
			input.Prefix = aws.String(prefix)
		}
		resp, err := client.ListObjectsV2(context.TODO(), input)
		require.NoError(t, err)
		keys := make([]string, 0, len(resp.Contents))
		for _, c := range resp.Contents {
			keys = append(keys, *c.Key)
		}
		return keys
	}
	deleteKey := func(key string) {
		t.Helper()
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
	}

	assert.ElementsMatch(t, []string{"backup/20260101/", "backup/20260102/"}, listPrefixes("backup/"))

	deleteKey("backup/20260101/manifest")
	assert.Equal(t, []string{"backup/20260102/"}, listPrefixes("backup/"),
		"the delete-marked date must drop out of CommonPrefixes")
	assert.Equal(t, []string{"backup/"}, listPrefixes(""),
		"the parent still holds a live object so it keeps its prefix")

	deleteKey("backup/20260102/manifest")
	assert.Empty(t, listPrefixes("backup/"), "no date is left under backup/")
	assert.Empty(t, listKeys("backup/"), "and backup/ itself is not a key")
	assert.Empty(t, listPrefixes(""), "the bucket lists as empty once every object is delete-marked")

	// The version history is untouched: both objects still have their version and their
	// delete marker, and removing a delete marker brings the prefix back.
	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, versions.Versions, 2)
	assert.Len(t, versions.DeleteMarkers, 2)

	for _, marker := range versions.DeleteMarkers {
		if *marker.Key != "backup/20260102/manifest" {
			continue
		}
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       marker.Key,
			VersionId: marker.VersionId,
		})
		require.NoError(t, err)
	}
	assert.Equal(t, []string{"backup/20260102/"}, listPrefixes("backup/"),
		"removing the delete marker restores the prefix")
}
