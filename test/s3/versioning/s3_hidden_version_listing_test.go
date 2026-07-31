package s3api

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A listing walks entries and drops the ones whose current version is a delete
// marker. When a whole run of consecutive entries drops out, the page it was
// filling can come back empty — and an empty page is easily mistaken for the end
// of the listing. Everything after the run then never appears, and the caller is
// told the objects do not exist. Backup repositories hit this shape constantly:
// they retract a batch of keys under one prefix and keep writing under the next.

func putObjectVersioned(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("x")),
	})
	require.NoError(t, err)
}

func listAllKeys(t *testing.T, client *s3.Client, bucket string, maxKeys int32) []string {
	t.Helper()
	var keys []string
	var token *string
	for {
		in := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), ContinuationToken: token}
		if maxKeys > 0 {
			in.MaxKeys = aws.Int32(maxKeys)
		}
		resp, err := client.ListObjectsV2(context.TODO(), in)
		require.NoError(t, err)
		for _, o := range resp.Contents {
			require.NotNil(t, o.Key)
			keys = append(keys, *o.Key)
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		require.NotNil(t, resp.NextContinuationToken, "a truncated listing must carry a continuation token")
		token = resp.NextContinuationToken
	}
	sort.Strings(keys)
	return keys
}

func TestListingSurvivesRunOfHiddenVersions(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// A long run of retracted keys, sorting before the live ones.
	const hidden = 25
	for i := 0; i < hidden; i++ {
		key := fmt.Sprintf("aretracted/%03d.lock", i)
		putObjectVersioned(t, client, bucketName, key)
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(key),
		})
		require.NoError(t, err)
	}

	var live []string
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("blive/%03d.blk", i)
		putObjectVersioned(t, client, bucketName, key)
		live = append(live, key)
	}
	sort.Strings(live)

	// Unpaginated: the retracted run must not hide what follows it.
	assert.Equal(t, live, listAllKeys(t, client, bucketName, 0),
		"a run of retracted keys must not truncate the listing")

	// Paginated with pages smaller than the retracted run, so at least one page
	// is filled entirely from entries that get dropped.
	for _, pageSize := range []int32{1, 2, 5, 10} {
		t.Run(fmt.Sprintf("maxKeys=%d", pageSize), func(t *testing.T) {
			assert.Equal(t, live, listAllKeys(t, client, bucketName, pageSize),
				"pagination across a retracted run must not lose the keys after it")
		})
	}
}

// The same shape with the retracted run in the middle, so both a preceding and a
// following key have to survive it.
func TestListingSurvivesHiddenVersionsBetweenLiveKeys(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	putObjectVersioned(t, client, bucketName, "a-first.blk")

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("m-retracted/%03d.lock", i)
		putObjectVersioned(t, client, bucketName, key)
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(key),
		})
		require.NoError(t, err)
	}

	putObjectVersioned(t, client, bucketName, "z-last.blk")

	want := []string{"a-first.blk", "z-last.blk"}
	for _, pageSize := range []int32{0, 1, 3, 10} {
		t.Run(fmt.Sprintf("maxKeys=%d", pageSize), func(t *testing.T) {
			assert.Equal(t, want, listAllKeys(t, client, bucketName, pageSize),
				"keys on both sides of a retracted run must both be listed")
		})
	}
}

// ListObjectVersions sees the same namespace from the other side: the retracted
// keys are still there as versions plus delete markers, and must all be reported.
func TestListObjectVersionsReportsRetractedKeys(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	const retracted = 10
	for i := 0; i < retracted; i++ {
		key := fmt.Sprintf("retracted/%03d.lock", i)
		putObjectVersioned(t, client, bucketName, key)
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(key),
		})
		require.NoError(t, err)
	}

	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	assert.Len(t, resp.Versions, retracted, "every written version must still be reported")
	assert.Len(t, resp.DeleteMarkers, retracted, "every retraction must be reported as a delete marker")

	// And the current-version view of the same namespace is empty.
	assert.Empty(t, listAllKeys(t, client, bucketName, 0),
		"no key should be current once every one has been retracted")
}
