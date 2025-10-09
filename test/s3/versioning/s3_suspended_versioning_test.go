package s3api

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestSuspendedVersioningNullOverwrite tests the scenario where:
// 1. Create object before versioning is enabled (pre-versioning object)
// 2. Enable versioning, then suspend it
// 3. Overwrite the object (should replace the null version, not create duplicate)
// 4. List versions should show only 1 version with versionId "null"
//
// This test corresponds to: test_versioning_obj_plain_null_version_overwrite_suspended
func TestSuspendedVersioningNullOverwrite(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	// Create bucket
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	objectKey := "testobjbar"

	// Step 1: Put object before versioning is configured (pre-versioning object)
	content1 := []byte("foooz")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(content1),
	})
	if err != nil {
		t.Fatalf("Failed to create pre-versioning object: %v", err)
	}
	t.Logf("Created pre-versioning object")

	// Step 2: Enable versioning
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	if err != nil {
		t.Fatalf("Failed to enable versioning: %v", err)
	}
	t.Logf("Enabled versioning")

	// Step 3: Suspend versioning
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusSuspended,
		},
	})
	if err != nil {
		t.Fatalf("Failed to suspend versioning: %v", err)
	}
	t.Logf("Suspended versioning")

	// Step 4: Overwrite the object during suspended versioning
	content2 := []byte("zzz")
	putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(content2),
	})
	if err != nil {
		t.Fatalf("Failed to overwrite object during suspended versioning: %v", err)
	}

	// Verify no VersionId is returned for suspended versioning
	if putResp.VersionId != nil {
		t.Errorf("Suspended versioning should NOT return VersionId, but got: %s", *putResp.VersionId)
	}
	t.Logf("Overwrote object during suspended versioning (no VersionId returned as expected)")

	// Step 5: Verify content is updated
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}
	defer getResp.Body.Close()

	gotContent := new(bytes.Buffer)
	gotContent.ReadFrom(getResp.Body)
	if !bytes.Equal(gotContent.Bytes(), content2) {
		t.Errorf("Expected content %q, got %q", content2, gotContent.Bytes())
	}
	t.Logf("Object content is correctly updated to: %q", content2)

	// Step 6: List object versions - should have only 1 version
	listResp, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to list object versions: %v", err)
	}

	// Count versions (excluding delete markers)
	versionCount := len(listResp.Versions)
	deleteMarkerCount := len(listResp.DeleteMarkers)

	t.Logf("List results: %d versions, %d delete markers", versionCount, deleteMarkerCount)
	for i, v := range listResp.Versions {
		t.Logf("  Version %d: Key=%s, VersionId=%s, IsLatest=%v, Size=%d",
			i, *v.Key, *v.VersionId, v.IsLatest, v.Size)
	}

	// THIS IS THE KEY ASSERTION: Should have exactly 1 version, not 2
	if versionCount != 1 {
		t.Errorf("Expected 1 version after suspended versioning overwrite, got %d versions", versionCount)
		t.Error("BUG: Duplicate null versions detected! The overwrite should have replaced the pre-versioning object.")
	} else {
		t.Logf("PASS: Only 1 version found (no duplicate null versions)")
	}

	if deleteMarkerCount != 0 {
		t.Errorf("Expected 0 delete markers, got %d", deleteMarkerCount)
	}

	// Verify the version has versionId "null"
	if versionCount > 0 {
		if listResp.Versions[0].VersionId == nil || *listResp.Versions[0].VersionId != "null" {
			t.Errorf("Expected VersionId to be 'null', got %v", listResp.Versions[0].VersionId)
		} else {
			t.Logf("Version ID is 'null' as expected")
		}
	}

	// Step 7: Delete the null version
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String("null"),
	})
	if err != nil {
		t.Fatalf("Failed to delete null version: %v", err)
	}
	t.Logf("Deleted null version")

	// Step 8: Verify object no longer exists
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err == nil {
		t.Error("Expected object to not exist after deleting null version")
	}
	t.Logf("Object no longer exists after deleting null version")

	// Step 9: Verify no versions remain
	listResp, err = client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to list object versions: %v", err)
	}

	if len(listResp.Versions) != 0 || len(listResp.DeleteMarkers) != 0 {
		t.Errorf("Expected no versions or delete markers, got %d versions and %d delete markers",
			len(listResp.Versions), len(listResp.DeleteMarkers))
	} else {
		t.Logf("No versions remain after deletion")
	}
}

// TestEnabledVersioningReturnsVersionId tests that when versioning is ENABLED,
// every PutObject operation returns a version ID
//
// This test corresponds to the create_multiple_versions helper function
func TestEnabledVersioningReturnsVersionId(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	// Create bucket
	bucketName := getNewBucketName()
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	objectKey := "testobj"

	// Enable versioning
	_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	if err != nil {
		t.Fatalf("Failed to enable versioning: %v", err)
	}
	t.Logf("Enabled versioning")

	// Create multiple versions
	numVersions := 3
	versionIds := make([]string, 0, numVersions)

	for i := 0; i < numVersions; i++ {
		content := []byte("content-" + string(rune('0'+i)))
		putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(content),
		})
		if err != nil {
			t.Fatalf("Failed to create version %d: %v", i, err)
		}

		// THIS IS THE KEY ASSERTION: VersionId MUST be returned for enabled versioning
		if putResp.VersionId == nil {
			t.Errorf("FAILED: PutObject with enabled versioning MUST return VersionId, but got nil for version %d", i)
		} else {
			versionId := *putResp.VersionId
			if versionId == "" {
				t.Errorf("FAILED: PutObject returned empty VersionId for version %d", i)
			} else if versionId == "null" {
				t.Errorf("FAILED: PutObject with enabled versioning should NOT return 'null' version ID, got: %s", versionId)
			} else {
				versionIds = append(versionIds, versionId)
				t.Logf("Version %d created with VersionId: %s", i, versionId)
			}
		}
	}

	if len(versionIds) != numVersions {
		t.Errorf("Expected %d version IDs, got %d", numVersions, len(versionIds))
	}

	// List versions to verify all were created
	listResp, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to list object versions: %v", err)
	}

	if len(listResp.Versions) != numVersions {
		t.Errorf("Expected %d versions in list, got %d", numVersions, len(listResp.Versions))
	} else {
		t.Logf("All %d versions are listed", numVersions)
	}

	// Verify all version IDs match
	for i, v := range listResp.Versions {
		t.Logf("  Version %d: VersionId=%s, Size=%d, IsLatest=%v", i, *v.VersionId, v.Size, v.IsLatest)
	}
}
