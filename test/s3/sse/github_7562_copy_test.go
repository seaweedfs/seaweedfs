package sse_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGitHub7562CopyFromEncryptedToTempToEncrypted reproduces the exact scenario from
// GitHub issue #7562: copying from an encrypted bucket to a temp bucket, then to another
// encrypted bucket fails with InternalError.
//
// Reproduction steps:
// 1. Create source bucket with SSE-S3 encryption enabled
// 2. Upload object (automatically encrypted)
// 3. Create temp bucket (no encryption)
// 4. Copy object from source to temp (decrypts)
// 5. Delete source bucket
// 6. Create destination bucket with SSE-S3 encryption
// 7. Copy object from temp to dest (should re-encrypt) - THIS FAILS
func TestGitHub7562CopyFromEncryptedToTempToEncrypted(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	// Create three buckets
	srcBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-src-")
	require.NoError(t, err, "Failed to create source bucket")

	tempBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-temp-")
	require.NoError(t, err, "Failed to create temp bucket")

	destBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-dest-")
	require.NoError(t, err, "Failed to create destination bucket")

	// Cleanup at the end
	defer func() {
		// Clean up in reverse order of creation
		cleanupTestBucket(ctx, client, destBucket)
		cleanupTestBucket(ctx, client, tempBucket)
		// Note: srcBucket is deleted during the test
	}()

	testData := []byte("Test data for GitHub issue #7562 - copy from encrypted to temp to encrypted bucket")
	objectKey := "demo-file.txt"

	t.Logf("[1] Creating source bucket with SSE-S3 default encryption: %s", srcBucket)

	// Step 1: Enable SSE-S3 default encryption on source bucket
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(srcBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set source bucket default encryption")

	t.Log("[2] Uploading demo object to source bucket")

	// Step 2: Upload object to source bucket (will be automatically encrypted)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
		// No encryption header - bucket default applies
	})
	require.NoError(t, err, "Failed to upload to source bucket")

	// Verify source object is encrypted
	srcHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD source object")
	assert.Equal(t, types.ServerSideEncryptionAes256, srcHead.ServerSideEncryption,
		"Source object should be SSE-S3 encrypted")
	t.Logf("Source object encryption: %v", srcHead.ServerSideEncryption)

	t.Logf("[3] Creating temp bucket (no encryption): %s", tempBucket)
	// Temp bucket already created without encryption

	t.Log("[4] Copying object from source to temp (should decrypt)")

	// Step 4: Copy to temp bucket (no encryption = decrypts)
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(tempBucket),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcBucket, objectKey)),
		// No encryption header - data stored unencrypted
	})
	require.NoError(t, err, "Failed to copy to temp bucket")

	// Verify temp object is NOT encrypted
	tempHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(tempBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD temp object")
	assert.Empty(t, tempHead.ServerSideEncryption, "Temp object should NOT be encrypted")
	t.Logf("Temp object encryption: %v (should be empty)", tempHead.ServerSideEncryption)

	// Verify temp object content
	tempGet, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(tempBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to GET temp object")
	tempData, err := io.ReadAll(tempGet.Body)
	tempGet.Body.Close()
	require.NoError(t, err, "Failed to read temp object")
	assertDataEqual(t, testData, tempData, "Temp object data should match original")

	t.Log("[5] Deleting original source bucket")

	// Step 5: Delete source bucket
	err = cleanupTestBucket(ctx, client, srcBucket)
	require.NoError(t, err, "Failed to delete source bucket")

	t.Logf("[6] Creating destination bucket with SSE-S3 encryption: %s", destBucket)

	// Step 6: Enable SSE-S3 default encryption on destination bucket
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(destBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set destination bucket default encryption")

	t.Log("[7] Copying object from temp to dest (should re-encrypt) - THIS IS WHERE #7562 FAILS")

	// Step 7: Copy from temp to dest bucket (should re-encrypt with SSE-S3)
	// THIS IS THE STEP THAT FAILS IN GITHUB ISSUE #7562
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", tempBucket, objectKey)),
		// No encryption header - bucket default should apply
	})
	require.NoError(t, err, "GitHub #7562: Failed to copy from temp to encrypted dest bucket")

	// Verify destination object is encrypted
	destHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD destination object")
	assert.Equal(t, types.ServerSideEncryptionAes256, destHead.ServerSideEncryption,
		"Destination object should be SSE-S3 encrypted via bucket default")
	t.Logf("Destination object encryption: %v", destHead.ServerSideEncryption)

	// Verify destination object content is correct
	destGet, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to GET destination object")
	destData, err := io.ReadAll(destGet.Body)
	destGet.Body.Close()
	require.NoError(t, err, "Failed to read destination object")
	assertDataEqual(t, testData, destData, "GitHub #7562: Destination object data mismatch after re-encryption")

	t.Log("[done] GitHub #7562 reproduction test completed successfully!")
}

// TestGitHub7562SimpleScenario tests the simpler variant: just copy unencrypted to encrypted bucket
func TestGitHub7562SimpleScenario(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	// Create two buckets
	srcBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-simple-src-")
	require.NoError(t, err, "Failed to create source bucket")
	defer cleanupTestBucket(ctx, client, srcBucket)

	destBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-simple-dest-")
	require.NoError(t, err, "Failed to create destination bucket")
	defer cleanupTestBucket(ctx, client, destBucket)

	testData := []byte("Simple test for unencrypted to encrypted copy")
	objectKey := "test-object.txt"

	t.Logf("Source bucket (no encryption): %s", srcBucket)
	t.Logf("Dest bucket (SSE-S3 default): %s", destBucket)

	// Upload to unencrypted source bucket
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload to source bucket")

	// Enable SSE-S3 on destination bucket
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(destBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set dest bucket encryption")

	// Copy to encrypted bucket (should use bucket default encryption)
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcBucket, objectKey)),
	})
	require.NoError(t, err, "Failed to copy to encrypted bucket")

	// Verify destination is encrypted
	destHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD dest object")
	assert.Equal(t, types.ServerSideEncryptionAes256, destHead.ServerSideEncryption,
		"Object should be encrypted via bucket default")

	// Verify content
	destGet, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to GET dest object")
	destData, err := io.ReadAll(destGet.Body)
	destGet.Body.Close()
	require.NoError(t, err, "Failed to read dest object")
	assertDataEqual(t, testData, destData, "Data mismatch")
}

// TestGitHub7562DebugMetadata helps debug what metadata is present on objects at each step
func TestGitHub7562DebugMetadata(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	// Create three buckets
	srcBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-debug-src-")
	require.NoError(t, err, "Failed to create source bucket")

	tempBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-debug-temp-")
	require.NoError(t, err, "Failed to create temp bucket")

	destBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-debug-dest-")
	require.NoError(t, err, "Failed to create destination bucket")

	defer func() {
		cleanupTestBucket(ctx, client, destBucket)
		cleanupTestBucket(ctx, client, tempBucket)
	}()

	testData := []byte("Debug metadata test for GitHub #7562")
	objectKey := "debug-file.txt"

	// Enable SSE-S3 on source
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(srcBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set source bucket encryption")

	// Upload
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload")

	// Log source object headers
	srcHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD source")
	t.Logf("=== SOURCE OBJECT (encrypted) ===")
	t.Logf("ServerSideEncryption: %v", srcHead.ServerSideEncryption)
	t.Logf("Metadata: %v", srcHead.Metadata)
	t.Logf("ContentLength: %d", aws.ToInt64(srcHead.ContentLength))

	// Copy to temp
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(tempBucket),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcBucket, objectKey)),
	})
	require.NoError(t, err, "Failed to copy to temp")

	// Log temp object headers
	tempHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(tempBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD temp")
	t.Logf("=== TEMP OBJECT (should be unencrypted) ===")
	t.Logf("ServerSideEncryption: %v (should be empty)", tempHead.ServerSideEncryption)
	t.Logf("Metadata: %v", tempHead.Metadata)
	t.Logf("ContentLength: %d", aws.ToInt64(tempHead.ContentLength))

	// Verify temp is NOT encrypted
	if tempHead.ServerSideEncryption != "" {
		t.Logf("WARNING: Temp object unexpectedly has encryption: %v", tempHead.ServerSideEncryption)
	}

	// Delete source bucket
	cleanupTestBucket(ctx, client, srcBucket)

	// Enable SSE-S3 on dest
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(destBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set dest bucket encryption")

	// Copy to dest - THIS IS WHERE #7562 FAILS
	t.Log("=== COPYING TO ENCRYPTED DEST ===")
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", tempBucket, objectKey)),
	})
	if err != nil {
		t.Logf("!!! COPY FAILED (GitHub #7562): %v", err)
		t.FailNow()
	}

	// Log dest object headers
	destHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD dest")
	t.Logf("=== DEST OBJECT (should be encrypted) ===")
	t.Logf("ServerSideEncryption: %v", destHead.ServerSideEncryption)
	t.Logf("Metadata: %v", destHead.Metadata)
	t.Logf("ContentLength: %d", aws.ToInt64(destHead.ContentLength))

	// Verify dest IS encrypted
	assert.Equal(t, types.ServerSideEncryptionAes256, destHead.ServerSideEncryption,
		"Dest object should be encrypted")

	// Verify content is readable
	destGet, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to GET dest")
	destData, err := io.ReadAll(destGet.Body)
	destGet.Body.Close()
	require.NoError(t, err, "Failed to read dest")
	assertDataEqual(t, testData, destData, "Data mismatch")

	t.Log("=== DEBUG TEST PASSED ===")
}

// TestGitHub7562LargeFile tests the issue with larger files that might trigger multipart handling
func TestGitHub7562LargeFile(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	srcBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-large-src-")
	require.NoError(t, err, "Failed to create source bucket")

	tempBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-large-temp-")
	require.NoError(t, err, "Failed to create temp bucket")

	destBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"7562-large-dest-")
	require.NoError(t, err, "Failed to create destination bucket")

	defer func() {
		cleanupTestBucket(ctx, client, destBucket)
		cleanupTestBucket(ctx, client, tempBucket)
	}()

	// Use larger file to potentially trigger different code paths
	testData := generateTestData(5 * 1024 * 1024) // 5MB
	objectKey := "large-file.bin"

	t.Logf("Testing with %d byte file", len(testData))

	// Enable SSE-S3 on source
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(srcBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set source bucket encryption")

	// Upload
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload")

	// Copy to temp (decrypt)
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(tempBucket),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcBucket, objectKey)),
	})
	require.NoError(t, err, "Failed to copy to temp")

	// Delete source
	cleanupTestBucket(ctx, client, srcBucket)

	// Enable SSE-S3 on dest
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(destBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set dest bucket encryption")

	// Copy to dest (re-encrypt) - GitHub #7562
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(objectKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", tempBucket, objectKey)),
	})
	require.NoError(t, err, "GitHub #7562: Large file copy to encrypted bucket failed")

	// Verify
	destHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to HEAD dest")
	assert.Equal(t, types.ServerSideEncryptionAes256, destHead.ServerSideEncryption)
	assert.Equal(t, int64(len(testData)), aws.ToInt64(destHead.ContentLength))

	// Verify content
	destGet, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to GET dest")
	destData, err := io.ReadAll(destGet.Body)
	destGet.Body.Close()
	require.NoError(t, err, "Failed to read dest")
	assertDataEqual(t, testData, destData, "Large file data mismatch")

	t.Log("Large file test passed!")
}
