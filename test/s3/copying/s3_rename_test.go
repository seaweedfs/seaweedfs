package copying_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createRenameSource builds the x-amz-rename-source value: the source bucket and
// key, URL encoded, the way x-amz-copy-source is built.
func createRenameSource(bucketName, key string) string {
	return fmt.Sprintf("/%s/%s", bucketName, url.PathEscape(key))
}

func renameObject(t *testing.T, client *s3.Client, bucketName, srcKey, dstKey string) {
	t.Helper()
	_, err := client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String(dstKey),
		RenameSource: aws.String(createRenameSource(bucketName, srcKey)),
	})
	require.NoError(t, err)
}

// requireRenameStatus asserts err carries the given HTTP status code.
func requireRenameStatus(t *testing.T, err error, status int) {
	t.Helper()
	require.Error(t, err)
	var respErr *smithyhttp.ResponseError
	require.True(t, errors.As(err, &respErr), "expected an HTTP response error, got %v", err)
	assert.Equal(t, status, respErr.HTTPStatusCode(), "unexpected error: %v", err)
}

func objectExists(t *testing.T, client *s3.Client, bucketName, key string) bool {
	t.Helper()
	_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	return err == nil
}

// TestRenameObject renames an object and checks the bytes, metadata and ETag
// arrive under the new key while the old key disappears.
func TestRenameObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	content := "rename me"
	put := putObjectWithMetadata(t, client, bucketName, "source.txt", content,
		map[string]string{"origin": "source"}, "text/plain")

	renameObject(t, client, bucketName, "source.txt", "renamed/target.txt")

	assert.False(t, objectExists(t, client, bucketName, "source.txt"), "source should be gone")

	resp := getObject(t, client, bucketName, "renamed/target.txt")
	assert.Equal(t, content, getObjectBody(t, resp))
	assert.Equal(t, "text/plain", aws.ToString(resp.ContentType))
	assert.Equal(t, "source", resp.Metadata["origin"])
	assert.Equal(t, aws.ToString(put.ETag), aws.ToString(resp.ETag), "ETag must survive the rename")
}

// TestRenameObjectOverwritesDestination: without a conditional header a rename
// replaces whatever the destination key held.
func TestRenameObjectOverwritesDestination(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "source.txt", "new content")
	putObject(t, client, bucketName, "target.txt", "old content")

	renameObject(t, client, bucketName, "source.txt", "target.txt")

	resp := getObject(t, client, bucketName, "target.txt")
	assert.Equal(t, "new content", getObjectBody(t, resp))
}

// TestRenameObjectIfNoneMatch: If-None-Match: * protects an existing destination.
func TestRenameObjectIfNoneMatch(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "source.txt", "new content")
	putObject(t, client, bucketName, "target.txt", "old content")

	_, err := client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:                 aws.String(bucketName),
		Key:                    aws.String("target.txt"),
		RenameSource:           aws.String(createRenameSource(bucketName, "source.txt")),
		DestinationIfNoneMatch: aws.String("*"),
	})
	requireRenameStatus(t, err, 412)

	resp := getObject(t, client, bucketName, "target.txt")
	assert.Equal(t, "old content", getObjectBody(t, resp))
	assert.True(t, objectExists(t, client, bucketName, "source.txt"), "a failed rename must leave the source alone")

	// The same rename onto a free key succeeds.
	_, err = client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:                 aws.String(bucketName),
		Key:                    aws.String("free.txt"),
		RenameSource:           aws.String(createRenameSource(bucketName, "source.txt")),
		DestinationIfNoneMatch: aws.String("*"),
	})
	require.NoError(t, err)
}

// TestRenameObjectSourceIfMatch gates the rename on the source's ETag.
func TestRenameObjectSourceIfMatch(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	put := putObject(t, client, bucketName, "source.txt", "content")

	_, err := client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String("target.txt"),
		RenameSource:  aws.String(createRenameSource(bucketName, "source.txt")),
		SourceIfMatch: aws.String("\"00000000000000000000000000000000\""),
	})
	requireRenameStatus(t, err, 412)
	assert.True(t, objectExists(t, client, bucketName, "source.txt"))

	_, err = client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String("target.txt"),
		RenameSource:  aws.String(createRenameSource(bucketName, "source.txt")),
		SourceIfMatch: put.ETag,
	})
	require.NoError(t, err)
	assert.False(t, objectExists(t, client, bucketName, "source.txt"))
	assert.True(t, objectExists(t, client, bucketName, "target.txt"))
}

// TestRenameObjectOntoDirectory: a key that already holds other objects is a
// directory, and an object must not be allowed to replace one.
func TestRenameObjectOntoDirectory(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "source.txt", "content")
	putObject(t, client, bucketName, "target/child.txt", "child")

	_, err := client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String("target"),
		RenameSource: aws.String(createRenameSource(bucketName, "source.txt")),
	})
	requireRenameStatus(t, err, 409)
	assert.True(t, objectExists(t, client, bucketName, "source.txt"))
	assert.True(t, objectExists(t, client, bucketName, "target/child.txt"))
}

// TestRenameObjectDirectorySource: a directory can be named without a trailing
// slash, and renaming one would move a whole subtree. It is not an object, so it
// is a missing key.
func TestRenameObjectDirectorySource(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	putObject(t, client, bucketName, "source/child.txt", "child")

	for _, src := range []string{"source", "source/"} {
		_, err := client.RenameObject(context.TODO(), &s3.RenameObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String("target.txt"),
			RenameSource: aws.String(createRenameSource(bucketName, src)),
		})
		requireRenameStatus(t, err, 404)
	}
	assert.True(t, objectExists(t, client, bucketName, "source/child.txt"))
	assert.False(t, objectExists(t, client, bucketName, "target.txt"))
}

// TestRenameObjectMissingSource reports a missing source as NoSuchKey.
func TestRenameObjectMissingSource(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	_, err := client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String("target.txt"),
		RenameSource: aws.String(createRenameSource(bucketName, "absent.txt")),
	})
	requireRenameStatus(t, err, 404)
}

// TestRenameObjectCrossBucket: the source must name the request's own bucket.
func TestRenameObjectCrossBucket(t *testing.T) {
	client := getS3Client(t)
	srcBucket := getNewBucketName()
	createBucket(t, client, srcBucket)
	defer deleteBucket(t, client, srcBucket)
	dstBucket := getNewBucketName()
	createBucket(t, client, dstBucket)
	defer deleteBucket(t, client, dstBucket)

	putObject(t, client, srcBucket, "source.txt", "content")

	_, err := client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:       aws.String(dstBucket),
		Key:          aws.String("target.txt"),
		RenameSource: aws.String(createRenameSource(srcBucket, "source.txt")),
	})
	requireRenameStatus(t, err, 400)
	assert.True(t, objectExists(t, client, srcBucket, "source.txt"))
}

// TestRenameObjectVersionedBucket: versioned buckets are not supported yet, and
// must say so rather than silently dropping versions.
func TestRenameObjectVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	_, err := client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)

	putObject(t, client, bucketName, "source.txt", "content")

	_, err = client.RenameObject(context.TODO(), &s3.RenameObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String("target.txt"),
		RenameSource: aws.String(createRenameSource(bucketName, "source.txt")),
	})
	requireRenameStatus(t, err, 501)
	assert.True(t, objectExists(t, client, bucketName, "source.txt"))
}
