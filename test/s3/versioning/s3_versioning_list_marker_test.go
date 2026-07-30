package s3api

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVersionedListMarkerIsExclusive covers start-after and marker on a versioned bucket,
// where an object is stored in a "<key>.versions" directory and so never matches the
// marker by entry name.
func TestVersionedListMarkerIsExclusive(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	for _, key := range []string{"file-0", "file-1", "file-2", "logs/file-0", "logs/file-1", "logs/file-2"} {
		putObject(t, client, bucketName, key, "content")
	}

	listV2 := func(prefix, startAfter string) []string {
		t.Helper()
		resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:     aws.String(bucketName),
			Prefix:     aws.String(prefix),
			StartAfter: aws.String(startAfter),
		})
		require.NoError(t, err)
		return contentKeys(resp.Contents)
	}
	listV1 := func(prefix, marker string) []string {
		t.Helper()
		resp, err := client.ListObjects(context.TODO(), &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
			Marker: aws.String(marker),
		})
		require.NoError(t, err)
		return contentKeys(resp.Contents)
	}

	assert.Equal(t, []string{"file-2"}, listV2("file-", "file-1"))
	assert.Equal(t, []string{"file-2"}, listV1("file-", "file-1"))
	assert.Equal(t, []string{"logs/file-2"}, listV2("logs/", "logs/file-1"))
	assert.Equal(t, []string{"logs/file-2"}, listV1("logs/", "logs/file-1"))
}

func contentKeys(contents []types.Object) []string {
	keys := make([]string, 0, len(contents))
	for _, c := range contents {
		keys = append(keys, *c.Key)
	}
	return keys
}
