package retention

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestReproduceObjectLockIssue reproduces the Object Lock header processing issue step by step
func TestReproduceObjectLockIssue(t *testing.T) {
	client := getS3Client(t)
	bucketName := fmt.Sprintf("object-lock-test-%d", time.Now().UnixNano())

	t.Logf("=== Reproducing Object Lock Header Processing Issue ===")
	t.Logf("Bucket name: %s", bucketName)

	// Step 1: Create bucket with Object Lock enabled header
	t.Logf("\n1. Creating bucket with ObjectLockEnabledForBucket=true")
	t.Logf("   This should send x-amz-bucket-object-lock-enabled: true header")

	createResp, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket:                     aws.String(bucketName),
		ObjectLockEnabledForBucket: aws.Bool(true), // This sets the x-amz-bucket-object-lock-enabled header
	})

	if err != nil {
		t.Fatalf("Bucket creation failed: %v", err)
	}
	t.Logf("Bucket created successfully")
	t.Logf("   Response: %+v", createResp)

	// Step 2: Check if Object Lock is actually enabled
	t.Logf("\n2. Checking Object Lock configuration to verify it was enabled")

	objectLockResp, err := client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		t.Logf("GetObjectLockConfiguration FAILED: %v", err)
		t.Logf("   This demonstrates the issue with header processing!")
		t.Logf("   S3 clients expect this call to succeed if Object Lock is supported")
		t.Logf("   When this fails, clients conclude that Object Lock is not supported")

		// This failure demonstrates the bug - the bucket was created but Object Lock wasn't enabled
		t.Logf("\nBUG CONFIRMED:")
		t.Logf("   - Bucket creation with ObjectLockEnabledForBucket=true succeeded")
		t.Logf("   - But GetObjectLockConfiguration fails")
		t.Logf("   - This means the x-amz-bucket-object-lock-enabled header was ignored")

	} else {
		t.Logf("GetObjectLockConfiguration succeeded!")
		t.Logf("   Response: %+v", objectLockResp)
		t.Logf("   Object Lock is properly enabled - this is the expected behavior")
	}

	// Step 3: Check versioning status (required for Object Lock)
	t.Logf("\n3. Checking bucket versioning status (required for Object Lock)")

	versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	t.Logf("   Versioning status: %v", versioningResp.Status)
	if versioningResp.Status != "Enabled" {
		t.Logf("   Versioning should be automatically enabled when Object Lock is enabled")
	}

	// Cleanup
	t.Logf("\n4. Cleaning up test bucket")
	_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("   Warning: Failed to delete bucket: %v", err)
	}

	t.Logf("\n=== Issue Reproduction Complete ===")
	t.Logf("Expected behavior after fix:")
	t.Logf("  - CreateBucket with ObjectLockEnabledForBucket=true should enable Object Lock")
	t.Logf("  - GetObjectLockConfiguration should return enabled configuration")
	t.Logf("  - Versioning should be automatically enabled")
}

// TestNormalBucketCreationStillWorks tests that normal bucket creation still works
func TestNormalBucketCreationStillWorks(t *testing.T) {
	client := getS3Client(t)
	bucketName := fmt.Sprintf("normal-test-%d", time.Now().UnixNano())

	t.Logf("=== Testing Normal Bucket Creation ===")

	// Create bucket without Object Lock
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	t.Logf("Normal bucket creation works")

	// Object Lock should NOT be enabled
	_, err = client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	require.Error(t, err, "GetObjectLockConfiguration should fail for bucket without Object Lock")
	t.Logf("GetObjectLockConfiguration correctly fails for normal bucket")

	// Cleanup
	client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
}
