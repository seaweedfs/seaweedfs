package s3api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestObjectLockValidation tests that S3 Object Lock functionality works end-to-end
// This test focuses on the complete Object Lock workflow that S3 clients expect
func TestObjectLockValidation(t *testing.T) {
	client := getS3Client(t)
	bucketName := fmt.Sprintf("object-lock-test-%d", time.Now().UnixNano())

	t.Logf("=== Validating S3 Object Lock Functionality ===")
	t.Logf("Bucket: %s", bucketName)

	// Step 1: Create bucket with Object Lock header
	t.Log("\n1. Creating bucket with x-amz-bucket-object-lock-enabled: true")
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket:                     aws.String(bucketName),
		ObjectLockEnabledForBucket: true, // This sends x-amz-bucket-object-lock-enabled: true
	})
	require.NoError(t, err, "Bucket creation should succeed")
	defer client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	t.Log("   ✅ Bucket created successfully")

	// Step 2: Check if Object Lock is supported (standard S3 client behavior)
	t.Log("\n2. Testing Object Lock support detection")
	_, err = client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "GetObjectLockConfiguration should succeed for Object Lock enabled bucket")
	t.Log("   ✅ GetObjectLockConfiguration succeeded - Object Lock is properly enabled")

	// Step 3: Verify versioning is enabled (required for Object Lock)
	t.Log("\n3. Verifying versioning is automatically enabled")
	versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	require.Equal(t, types.BucketVersioningStatusEnabled, versioningResp.Status, "Versioning should be automatically enabled")
	t.Log("   ✅ Versioning automatically enabled")

	// Step 4: Test actual Object Lock functionality
	t.Log("\n4. Testing Object Lock retention functionality")

	// Create an object
	key := "protected-object.dat"
	content := "Important data that needs immutable protection"
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.VersionId, "Object should have a version ID")
	t.Log("   ✅ Object created with versioning")

	// Apply Object Lock retention
	retentionUntil := time.Now().Add(24 * time.Hour)
	_, err = client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeCompliance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err, "Setting Object Lock retention should succeed")
	t.Log("   ✅ Object Lock retention applied successfully")

	// Verify retention is in effect
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.Error(t, err, "Object should be protected by retention and cannot be deleted")
	t.Log("   ✅ Object is properly protected by retention policy")

	// Verify we can read the object (should still work)
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "Reading protected object should still work")
	defer getResp.Body.Close()
	t.Log("   ✅ Protected object can still be read")

	t.Log("\n🎉 S3 OBJECT LOCK VALIDATION SUCCESSFUL!")
	t.Log("   - Bucket creation with Object Lock header works")
	t.Log("   - Object Lock support detection works (GetObjectLockConfiguration succeeds)")
	t.Log("   - Versioning is automatically enabled")
	t.Log("   - Object Lock retention functionality works")
	t.Log("   - Objects are properly protected from deletion")
	t.Log("")
	t.Log("✅ S3 clients will now recognize SeaweedFS as supporting Object Lock!")
}
