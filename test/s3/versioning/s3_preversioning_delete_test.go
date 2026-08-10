package s3api

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Deleting a key whose null version predates versioning records the delete
// marker under <key>.versions but leaves the null object at the base path.
// The listing must retract that stale entry, on whichever page it lands.

func deleteObject(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	resp, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)
	require.True(t, resp.DeleteMarker != nil && *resp.DeleteMarker, "the delete must record a delete marker")
}

func TestPreVersioningNullObjectDeleteHidesKey(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "k.txt", "pre-versioning")
	enableVersioning(t, client, bucketName)
	deleteObject(t, client, bucketName, "k.txt")

	assert.Empty(t, listAllKeys(t, client, bucketName, 0), "the deleted key must leave the listing")

	_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String("k.txt"),
	})
	assert.Error(t, err, "the deleted key must not be readable")

	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	require.Len(t, versions.DeleteMarkers, 1)
	assert.True(t, *versions.DeleteMarkers[0].IsLatest, "the delete marker is the current version")
	require.Len(t, versions.Versions, 1)
	assert.Equal(t, "null", *versions.Versions[0].VersionId)
	assert.False(t, *versions.Versions[0].IsLatest, "the null version is shadowed by the marker")
}

func TestPreVersioningNullObjectDeleteAfterOverwriteHidesKey(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "k.txt", "pre-versioning")
	enableVersioning(t, client, bucketName)
	putObject(t, client, bucketName, "k.txt", "versioned overwrite")
	deleteObject(t, client, bucketName, "k.txt")

	assert.Empty(t, listAllKeys(t, client, bucketName, 0), "the deleted key must leave the listing")
}

// The retraction and the replacement must both survive a page boundary landing
// between a key and its .versions sibling.
func TestPreVersioningNullObjectAcrossPageBoundary(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "a.txt", "old")
	enableVersioning(t, client, bucketName)
	deleteObject(t, client, bucketName, "a.txt")
	putObjectVersioned(t, client, bucketName, "b.txt")

	assert.Equal(t, []string{"b.txt"}, listAllKeys(t, client, bucketName, 1),
		"a page ending on the stale null object must still retract it")
}

func TestPreVersioningNullObjectMetadataAcrossPageBoundary(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	overwrite := "versioned overwrite"
	putObject(t, client, bucketName, "a.txt", "old")
	enableVersioning(t, client, bucketName)
	putObject(t, client, bucketName, "a.txt", overwrite)
	putObjectVersioned(t, client, bucketName, "b.txt")

	page, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName), MaxKeys: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page.Contents, 1)
	assert.Equal(t, "a.txt", *page.Contents[0].Key)
	assert.Equal(t, int64(len(overwrite)), *page.Contents[0].Size,
		"a page ending on the null object must still pick up the current version's metadata")
}

// A key such as "a.txt.bak" sorts between "a.txt" and "a.txt.versions", so the
// sibling's outcome arrives entries later, possibly on a later page.
func TestPreVersioningInterveningKeyRetraction(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "a.txt", "pre-versioning")
	putObject(t, client, bucketName, "a.txt!between", "pre-versioning")
	putObject(t, client, bucketName, "a.txt.bak", "pre-versioning")
	enableVersioning(t, client, bucketName)
	deleteObject(t, client, bucketName, "a.txt")

	for _, maxKeys := range []int32{0, 1, 2} {
		assert.Equal(t, []string{"a.txt!between", "a.txt.bak"}, listAllKeys(t, client, bucketName, maxKeys),
			"maxKeys=%d must not list the deleted key", maxKeys)
	}
}

func TestPreVersioningInterveningKeyMetadata(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	overwrite := "versioned overwrite"
	putObject(t, client, bucketName, "a.txt", "old")
	putObject(t, client, bucketName, "a.txt!between", "pre-versioning")
	putObject(t, client, bucketName, "a.txt.bak", "pre-versioning")
	enableVersioning(t, client, bucketName)
	putObject(t, client, bucketName, "a.txt", overwrite)

	for _, maxKeys := range []int32{1, 2} {
		var keys []string
		var token *string
		for {
			page, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket: aws.String(bucketName), MaxKeys: aws.Int32(maxKeys), ContinuationToken: token,
			})
			require.NoError(t, err)
			for _, o := range page.Contents {
				keys = append(keys, *o.Key)
				if *o.Key == "a.txt" {
					assert.Equal(t, int64(len(overwrite)), *o.Size,
						"maxKeys=%d must list the current version's metadata", maxKeys)
				}
			}
			if page.IsTruncated == nil || !*page.IsTruncated {
				break
			}
			token = page.NextContinuationToken
		}
		assert.Equal(t, []string{"a.txt", "a.txt!between", "a.txt.bak"}, keys, "maxKeys=%d", maxKeys)
	}
}

// CommonPrefixes derive from listable keys, so deleting the only pre-versioning
// object under a prefix takes the prefix with it.
func TestPreVersioningDeletedPrefixHidesCommonPrefix(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "p/k.txt", "pre-versioning")
	enableVersioning(t, client, bucketName)
	deleteObject(t, client, bucketName, "p/k.txt")

	page, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName), Delimiter: aws.String("/"),
	})
	require.NoError(t, err)
	assert.Empty(t, page.Contents)
	assert.Empty(t, page.CommonPrefixes, "no listable key remains under p/")
}

// Nested names (k, k!, k!!, ...) can hold more unresolved null objects than the
// pending cap; the evicted key must still be settled, on any page size.
func TestPreVersioningNestedNullObjectsBeyondCap(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	var live []string
	for i := 0; i < 10; i++ {
		key := "k" + strings.Repeat("!", i)
		putObject(t, client, bucketName, key, "pre-versioning")
		if i > 0 {
			live = append(live, key)
		}
	}
	enableVersioning(t, client, bucketName)
	deleteObject(t, client, bucketName, "k")

	sort.Strings(live)
	for _, maxKeys := range []int32{0, 3, 9} {
		assert.Equal(t, live, listAllKeys(t, client, bucketName, maxKeys),
			"maxKeys=%d must not list the deleted key", maxKeys)
	}
}

// A suspended-versioning write is the current null version; its .versions
// sibling (whose latest pointer the write cleared) must not list it a second
// time or resurrect an older version's metadata.
func TestSuspendedNullObjectListsOnce(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)
	putObject(t, client, bucketName, "k.txt", "versioned")
	suspendVersioning(t, client, bucketName)
	current := "suspended current"
	putObject(t, client, bucketName, "k.txt", current)

	page, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	require.Len(t, page.Contents, 1, "one key must list exactly once")
	assert.Equal(t, int64(len(current)), *page.Contents[0].Size, "the null version is current")
}
