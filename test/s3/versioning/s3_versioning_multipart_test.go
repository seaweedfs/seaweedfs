package s3api

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultipartUploadVersioningListETag tests that multipart uploaded objects
// in versioned buckets have correct ETags when listed.
// This covers a bug where synthetic entries for versioned objects didn't include
// proper ETag handling for multipart uploads (ETags with format "<md5>-<parts>").
func TestMultipartUploadVersioningListETag(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Enable versioning
	_, err := client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err, "Failed to enable versioning")

	// Create multipart upload
	objectKey := "multipart-test-object"
	createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to create multipart upload")

	uploadId := *createResp.UploadId

	// Upload 2 parts (minimum 5MB per part except last)
	partSize := 5 * 1024 * 1024 // 5MB
	part1Data := bytes.Repeat([]byte("a"), partSize)
	part2Data := bytes.Repeat([]byte("b"), partSize)

	// Calculate MD5 for each part
	part1MD5 := md5.Sum(part1Data)
	part2MD5 := md5.Sum(part2Data)

	// Upload part 1
	uploadPart1Resp, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err, "Failed to upload part 1")

	// Upload part 2
	uploadPart2Resp, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err, "Failed to upload part 2")

	// Complete multipart upload
	completeResp, err := client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       uploadPart1Resp.ETag,
					PartNumber: aws.Int32(1),
				},
				{
					ETag:       uploadPart2Resp.ETag,
					PartNumber: aws.Int32(2),
				},
			},
		},
	})
	require.NoError(t, err, "Failed to complete multipart upload")

	// Verify the ETag from CompleteMultipartUpload has the multipart format (md5-parts)
	completeETag := strings.Trim(*completeResp.ETag, "\"")
	assert.Contains(t, completeETag, "-", "Multipart ETag should contain '-' (format: md5-parts)")
	assert.True(t, strings.HasSuffix(completeETag, "-2"), "Multipart ETag should end with '-2' for 2 parts")

	t.Logf("CompleteMultipartUpload ETag: %s", completeETag)
	t.Logf("Part 1 MD5: %x", part1MD5)
	t.Logf("Part 2 MD5: %x", part2MD5)

	// HeadObject should return the same ETag
	headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to head object")

	headETag := strings.Trim(*headResp.ETag, "\"")
	assert.Equal(t, completeETag, headETag, "HeadObject ETag should match CompleteMultipartUpload ETag")

	// ListObjectsV2 should return the same ETag
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to list objects")
	require.Len(t, listResp.Contents, 1, "Should have exactly one object")

	listETag := strings.Trim(*listResp.Contents[0].ETag, "\"")
	assert.Equal(t, completeETag, listETag, "ListObjectsV2 ETag should match CompleteMultipartUpload ETag")
	assert.NotEmpty(t, listETag, "ListObjectsV2 ETag should not be empty")

	t.Logf("ListObjectsV2 ETag: %s", listETag)

	// ListObjectVersions should also return the correct ETag
	versionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to list object versions")
	require.Len(t, versionsResp.Versions, 1, "Should have exactly one version")

	versionETag := strings.Trim(*versionsResp.Versions[0].ETag, "\"")
	assert.Equal(t, completeETag, versionETag, "ListObjectVersions ETag should match CompleteMultipartUpload ETag")
	assert.NotEmpty(t, versionETag, "ListObjectVersions ETag should not be empty")

	t.Logf("ListObjectVersions ETag: %s", versionETag)
}

// TestMultipartUploadMultipleVersionsListETag tests that multiple versions
// of multipart uploaded objects all have correct ETags when listed.
func TestMultipartUploadMultipleVersionsListETag(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Enable versioning
	_, err := client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err, "Failed to enable versioning")

	objectKey := "multipart-multi-version-object"
	partSize := 5 * 1024 * 1024 // 5MB
	var expectedETags []string

	// Create 3 versions using multipart upload
	for version := 1; version <= 3; version++ {
		// Create multipart upload
		createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to create multipart upload for version %d", version)

		uploadId := *createResp.UploadId

		// Create unique data for each version
		partData := bytes.Repeat([]byte(fmt.Sprintf("%d", version)), partSize)

		// Upload single part (still results in multipart ETag format)
		uploadPartResp, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   aws.String(uploadId),
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err, "Failed to upload part for version %d", version)

		// Complete multipart upload
		completeResp, err := client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: aws.String(uploadId),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						ETag:       uploadPartResp.ETag,
						PartNumber: aws.Int32(1),
					},
				},
			},
		})
		require.NoError(t, err, "Failed to complete multipart upload for version %d", version)

		etag := strings.Trim(*completeResp.ETag, "\"")
		expectedETags = append(expectedETags, etag)
		t.Logf("Version %d ETag: %s", version, etag)
	}

	// ListObjectVersions should return all versions with correct ETags
	versionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to list object versions")
	require.Len(t, versionsResp.Versions, 3, "Should have exactly 3 versions")

	// Collect ETags from the listing
	var listedETags []string
	for _, v := range versionsResp.Versions {
		etag := strings.Trim(*v.ETag, "\"")
		listedETags = append(listedETags, etag)
		assert.NotEmpty(t, etag, "Version ETag should not be empty")
		assert.Contains(t, etag, "-", "Multipart ETag should contain '-'")
	}

	t.Logf("Expected ETags: %v", expectedETags)
	t.Logf("Listed ETags: %v", listedETags)

	// Verify all expected ETags are present (order may differ due to version ordering)
	for _, expected := range expectedETags {
		found := false
		for _, listed := range listedETags {
			if expected == listed {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected ETag %s should be in listed ETags", expected)
	}

	// Regular ListObjectsV2 should return only the latest version with correct ETag
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to list objects")
	require.Len(t, listResp.Contents, 1, "Should have exactly one object in regular listing")

	listETag := strings.Trim(*listResp.Contents[0].ETag, "\"")
	// The latest version (version 3) should be the one shown
	assert.Equal(t, expectedETags[2], listETag, "ListObjectsV2 should show latest version's ETag")
}

// TestMixedSingleAndMultipartVersionsListETag tests that a mix of
// single-part and multipart uploaded versions all have correct ETags.
func TestMixedSingleAndMultipartVersionsListETag(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Enable versioning
	_, err := client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err, "Failed to enable versioning")

	objectKey := "mixed-upload-versions"

	// Version 1: Regular PutObject (single-part, pure MD5 ETag)
	content1 := []byte("This is version 1 content - single part upload")
	putResp1, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(content1),
	})
	require.NoError(t, err, "Failed to put version 1")
	etag1 := strings.Trim(*putResp1.ETag, "\"")
	assert.NotContains(t, etag1, "-", "Single-part ETag should not contain '-'")
	t.Logf("Version 1 (PutObject) ETag: %s", etag1)

	// Version 2: Multipart upload
	partSize := 5 * 1024 * 1024
	partData := bytes.Repeat([]byte("x"), partSize)

	createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to create multipart upload")

	uploadPartResp, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   createResp.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(partData),
	})
	require.NoError(t, err, "Failed to upload part")

	completeResp, err := client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: createResp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       uploadPartResp.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	require.NoError(t, err, "Failed to complete multipart upload")
	etag2 := strings.Trim(*completeResp.ETag, "\"")
	assert.Contains(t, etag2, "-", "Multipart ETag should contain '-'")
	t.Logf("Version 2 (Multipart) ETag: %s", etag2)

	// Version 3: Another regular PutObject
	content3 := []byte("This is version 3 content - another single part upload")
	putResp3, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(content3),
	})
	require.NoError(t, err, "Failed to put version 3")
	etag3 := strings.Trim(*putResp3.ETag, "\"")
	assert.NotContains(t, etag3, "-", "Single-part ETag should not contain '-'")
	t.Logf("Version 3 (PutObject) ETag: %s", etag3)

	// ListObjectVersions should return all 3 versions with correct ETags
	versionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to list object versions")
	require.Len(t, versionsResp.Versions, 3, "Should have exactly 3 versions")

	expectedETags := map[string]bool{etag1: false, etag2: false, etag3: false}

	for _, v := range versionsResp.Versions {
		etag := strings.Trim(*v.ETag, "\"")
		assert.NotEmpty(t, etag, "Version ETag should not be empty")

		if _, exists := expectedETags[etag]; exists {
			expectedETags[etag] = true
		}
		t.Logf("Listed version %s ETag: %s, IsLatest: %v", *v.VersionId, etag, *v.IsLatest)
	}

	// Verify all ETags were found
	for etag, found := range expectedETags {
		assert.True(t, found, "ETag %s should be in listed versions", etag)
	}

	// Regular ListObjectsV2 should return only the latest (version 3)
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to list objects")
	require.Len(t, listResp.Contents, 1, "Should have exactly one object")

	listETag := strings.Trim(*listResp.Contents[0].ETag, "\"")
	assert.Equal(t, etag3, listETag, "ListObjectsV2 should show latest version's ETag (version 3)")
}

