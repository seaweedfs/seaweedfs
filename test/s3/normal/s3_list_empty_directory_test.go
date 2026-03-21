package example

import (
	"bytes"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3ListObjectsEmptyDirectoryMarkers reproduces GitHub issue #8698:
// S3 API ListObjects does not include empty directory markers (zero-byte
// objects with keys ending in "/") created via PutObject.
//
// AWS S3 includes these markers as regular keys in Contents. SeaweedFS
// was filtering them out during recursive directory listing.
func TestS3ListObjectsEmptyDirectoryMarkers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	bucketName := createTestBucket(t, cluster, "test-empty-dirs-")

	// Create a regular file at Empty/manifest.yaml
	_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("Empty/manifest.yaml"),
		Body:   bytes.NewReader([]byte("name: test\nversion: 1.0\n")),
	})
	require.NoError(t, err, "failed to create regular file")

	// Create an empty directory marker via PutObject (key ending in "/", zero bytes).
	// This is how `aws s3api put-object --key "Empty/empty/"` works.
	_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("Empty/empty/"),
		Body:   bytes.NewReader([]byte{}),
	})
	require.NoError(t, err, "failed to create empty directory marker")

	// Verify the directory marker exists via HeadObject
	_, err = cluster.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("Empty/empty/"),
	})
	require.NoError(t, err, "directory marker should exist via HeadObject")

	// Test 1: ListObjectsV2 with prefix (no delimiter) — the exact scenario from the issue.
	// AWS S3 returns both "Empty/empty/" and "Empty/manifest.yaml" in Contents.
	t.Run("ListV2_WithPrefix_NoDelimiter", func(t *testing.T) {
		resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
			Prefix: aws.String("Empty"),
		})
		require.NoError(t, err)

		keys := collectKeys(resp.Contents)
		sort.Strings(keys)

		assert.Equal(t, []string{"Empty/empty/", "Empty/manifest.yaml"}, keys,
			"both the directory marker and regular file should be listed")
	})

	// Test 2: ListObjectsV1 with prefix (no delimiter).
	t.Run("ListV1_WithPrefix_NoDelimiter", func(t *testing.T) {
		resp, err := cluster.s3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String("Empty"),
		})
		require.NoError(t, err)

		keys := collectKeysV1(resp.Contents)
		sort.Strings(keys)

		assert.Equal(t, []string{"Empty/empty/", "Empty/manifest.yaml"}, keys,
			"both the directory marker and regular file should be listed")
	})

	// Test 3: ListObjectsV2 without prefix and without delimiter.
	// All objects in the bucket should appear, including directory markers.
	t.Run("ListV2_NoPrefix_NoDelimiter", func(t *testing.T) {
		resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)

		keys := collectKeys(resp.Contents)

		assert.Contains(t, keys, "Empty/empty/", "directory marker should appear in listing")
		assert.Contains(t, keys, "Empty/manifest.yaml", "regular file should appear in listing")
	})

	// Test 4: ListObjectsV2 with prefix "Empty/" and delimiter "/".
	// The directory marker "Empty/empty/" should appear as a CommonPrefix,
	// and "Empty/manifest.yaml" should appear in Contents.
	t.Run("ListV2_WithPrefix_WithDelimiter", func(t *testing.T) {
		resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Prefix:    aws.String("Empty/"),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)

		keys := collectKeys(resp.Contents)
		prefixes := collectPrefixes(resp.CommonPrefixes)

		assert.Contains(t, keys, "Empty/manifest.yaml", "regular file should be in Contents")
		assert.Contains(t, prefixes, "Empty/empty/", "directory marker should appear as CommonPrefix")
	})

	// Test 5: Multiple empty directory markers at different nesting levels.
	t.Run("ListV2_NestedEmptyDirs", func(t *testing.T) {
		nestedBucket := createTestBucket(t, cluster, "test-nested-dirs-")

		// Create nested structure:
		//   dir/sub1/         (empty dir marker)
		//   dir/sub2/deep/    (empty dir marker at deeper level)
		//   dir/file.txt      (regular file)
		_, err := cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(nestedBucket),
			Key:    aws.String("dir/sub1/"),
			Body:   bytes.NewReader([]byte{}),
		})
		require.NoError(t, err)

		_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(nestedBucket),
			Key:    aws.String("dir/sub2/deep/"),
			Body:   bytes.NewReader([]byte{}),
		})
		require.NoError(t, err)

		_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(nestedBucket),
			Key:    aws.String("dir/file.txt"),
			Body:   bytes.NewReader([]byte("content")),
		})
		require.NoError(t, err)

		resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(nestedBucket),
		})
		require.NoError(t, err)

		keys := collectKeys(resp.Contents)
		sort.Strings(keys)

		assert.Equal(t, []string{"dir/file.txt", "dir/sub1/", "dir/sub2/deep/"}, keys,
			"all objects including nested empty directory markers should be listed")
	})

	// Test 6: Directory marker with a child object, paginated with MaxKeys=1.
	// This exercises the emit-marker-then-recurse path under truncation:
	// the marker is emitted, maxKeys drops to 0, and the child must be
	// picked up on a subsequent page.
	t.Run("ListV2_MarkerWithChild_Paginated", func(t *testing.T) {
		siblingBucket := createTestBucket(t, cluster, "test-sibling-dirs-")

		// Create:
		//   docs/            (directory key object)
		//   docs/readme.txt  (child file inside the marker)
		//   readme.txt       (sibling file at top level)
		_, err := cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(siblingBucket),
			Key:    aws.String("docs/"),
			Body:   bytes.NewReader([]byte{}),
		})
		require.NoError(t, err)

		_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(siblingBucket),
			Key:    aws.String("docs/readme.txt"),
			Body:   bytes.NewReader([]byte("inside docs")),
		})
		require.NoError(t, err)

		_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(siblingBucket),
			Key:    aws.String("readme.txt"),
			Body:   bytes.NewReader([]byte("hello")),
		})
		require.NoError(t, err)

		// Unpaginated: all three keys should appear.
		resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(siblingBucket),
		})
		require.NoError(t, err)

		keys := collectKeys(resp.Contents)
		sort.Strings(keys)

		assert.Equal(t, []string{"docs/", "docs/readme.txt", "readme.txt"}, keys,
			"directory marker, its child, and sibling file should all be listed")

		// Paginated with MaxKeys=1: collect all keys across pages.
		var allKeys []string
		var token *string
		for {
			pageResp, pageErr := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
				Bucket:            aws.String(siblingBucket),
				MaxKeys:           aws.Int64(1),
				ContinuationToken: token,
			})
			require.NoError(t, pageErr)
			allKeys = append(allKeys, collectKeys(pageResp.Contents)...)
			if !aws.BoolValue(pageResp.IsTruncated) {
				break
			}
			token = pageResp.NextContinuationToken
		}
		sort.Strings(allKeys)

		assert.Equal(t, []string{"docs/", "docs/readme.txt", "readme.txt"}, allKeys,
			"paginated listing should return all keys including marker and its child")
	})

	t.Run("ListExplicitDirectoryMarkerWithOctetStreamContentType", func(t *testing.T) {
		bucketWithContentType := createTestBucket(t, cluster, "test-explicit-dir-")
		objectKey := "test-content/empty/"

		_, err := cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket:      aws.String(bucketWithContentType),
			Key:         aws.String(objectKey),
			Body:        bytes.NewReader([]byte{}),
			ContentType: aws.String("application/octet-stream"),
		})
		require.NoError(t, err, "failed to create explicit directory marker with octet-stream content type")

		headResp, err := cluster.s3Client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucketWithContentType),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "directory marker should exist via HeadObject")
		assert.Equal(t, "application/octet-stream", aws.StringValue(headResp.ContentType))

		respV1, err := cluster.s3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketWithContentType),
			Prefix: aws.String("test-content"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{objectKey}, collectKeysV1(respV1.Contents),
			"explicit empty directory markers with octet-stream content type should be listed")

		respV2, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucketWithContentType),
			Prefix: aws.String("test-content"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{objectKey}, collectKeys(respV2.Contents),
			"explicit empty directory markers with octet-stream content type should be listed")
	})
}

func collectKeys(contents []*s3.Object) []string {
	keys := make([]string, 0, len(contents))
	for _, obj := range contents {
		keys = append(keys, aws.StringValue(obj.Key))
	}
	return keys
}

func collectKeysV1(contents []*s3.Object) []string {
	return collectKeys(contents)
}

func collectPrefixes(prefixes []*s3.CommonPrefix) []string {
	result := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		result = append(result, aws.StringValue(p.Prefix))
	}
	return result
}
