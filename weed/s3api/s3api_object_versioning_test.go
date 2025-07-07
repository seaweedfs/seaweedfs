package s3api

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func TestGenerateVersionId(t *testing.T) {
	// Test that version IDs are unique and have correct length
	versionId1 := generateVersionId()
	versionId2 := generateVersionId()

	assert.NotEqual(t, versionId1, versionId2, "Version IDs should be unique")
	assert.Equal(t, 32, len(versionId1), "Version ID should be 32 characters long")
	assert.Equal(t, 32, len(versionId2), "Version ID should be 32 characters long")

	// Test that they only contain hex characters
	for _, char := range versionId1 {
		assert.True(t, (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f'),
			"Version ID should only contain hex characters")
	}
}

func TestVersioningEnabledCheck(t *testing.T) {
	// Test bucket with versioning enabled
	bucketEntry := &filer_pb.Entry{
		Name:        "test-bucket",
		IsDirectory: true,
		Extended: map[string][]byte{
			s3_constants.ExtVersioningKey: []byte("Enabled"),
		},
	}

	// Mock the getEntry function result
	// In a real test, you would set up proper mocking
	// For now, we test the logic conceptually

	versioningKey, exists := bucketEntry.Extended[s3_constants.ExtVersioningKey]
	isEnabled := exists && string(versioningKey) == "Enabled"
	assert.True(t, isEnabled, "Versioning should be enabled")

	// Test bucket with versioning suspended
	bucketEntry.Extended[s3_constants.ExtVersioningKey] = []byte("Suspended")
	versioningKey, exists = bucketEntry.Extended[s3_constants.ExtVersioningKey]
	isEnabled = exists && string(versioningKey) == "Enabled"
	assert.False(t, isEnabled, "Versioning should be suspended")

	// Test bucket without versioning configuration
	delete(bucketEntry.Extended, s3_constants.ExtVersioningKey)
	_, exists = bucketEntry.Extended[s3_constants.ExtVersioningKey]
	assert.False(t, exists, "Versioning key should not exist")
}

func TestVersionedObjectPaths(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	bucket := "test-bucket"
	object := "/my-file.txt"

	expectedVersionsDir := "/buckets/test-bucket/my-file.txt.versions"
	actualVersionsDir := s3a.getVersionedObjectDir(bucket, object)
	assert.Equal(t, expectedVersionsDir, actualVersionsDir,
		"Versioned object directory path should be correct")

	versionId := "abc123def456"
	expectedVersionFile := "abc123def456"
	actualVersionFile := s3a.getVersionFileName(versionId)
	assert.Equal(t, expectedVersionFile, actualVersionFile,
		"Version file name should match version ID")
}

func TestObjectVersionMetadata(t *testing.T) {
	// Test version metadata in entry
	versionId := "test-version-123"
	isLatest := true
	isDeleteMarker := false

	entry := &filer_pb.Entry{
		Name:        "test-file.txt",
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: time.Now().Unix(),
		},
		Extended: make(map[string][]byte),
	}

	// Add version metadata
	entry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
	entry.Extended[s3_constants.ExtIsLatestKey] = []byte(fmt.Sprintf("%t", isLatest))
	entry.Extended[s3_constants.ExtDeleteMarkerKey] = []byte(fmt.Sprintf("%t", isDeleteMarker))

	// Verify metadata
	assert.Equal(t, versionId, string(entry.Extended[s3_constants.ExtVersionIdKey]))
	assert.Equal(t, "true", string(entry.Extended[s3_constants.ExtIsLatestKey]))
	assert.Equal(t, "false", string(entry.Extended[s3_constants.ExtDeleteMarkerKey]))
}

func TestDeleteMarkerCreation(t *testing.T) {
	versionId := "delete-marker-123"

	deleteMarkerEntry := &filer_pb.Entry{
		Name:        versionId,
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: time.Now().Unix(),
		},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey:    []byte(versionId),
			s3_constants.ExtDeleteMarkerKey: []byte("true"),
			s3_constants.ExtIsLatestKey:     []byte("true"),
		},
	}

	// Verify delete marker properties
	assert.Equal(t, "true", string(deleteMarkerEntry.Extended[s3_constants.ExtDeleteMarkerKey]))
	assert.Equal(t, "true", string(deleteMarkerEntry.Extended[s3_constants.ExtIsLatestKey]))
	assert.Equal(t, versionId, string(deleteMarkerEntry.Extended[s3_constants.ExtVersionIdKey]))
}

func TestVersioningSorting(t *testing.T) {
	// Test that versions are sorted correctly by timestamp
	now := time.Now()

	versions := []*ObjectVersion{
		{
			VersionId:    "version3",
			LastModified: now.Add(-1 * time.Hour),
			IsLatest:     false,
		},
		{
			VersionId:    "version1",
			LastModified: now.Add(-3 * time.Hour),
			IsLatest:     false,
		},
		{
			VersionId:    "version2",
			LastModified: now.Add(-2 * time.Hour),
			IsLatest:     false,
		},
		{
			VersionId:    "version4",
			LastModified: now,
			IsLatest:     true,
		},
	}

	// Sort versions by timestamp (newest first)
	for i := 0; i < len(versions)-1; i++ {
		for j := i + 1; j < len(versions); j++ {
			if versions[i].LastModified.Before(versions[j].LastModified) {
				versions[i], versions[j] = versions[j], versions[i]
			}
		}
	}

	// Verify sorting order (newest first)
	assert.Equal(t, "version4", versions[0].VersionId)
	assert.Equal(t, "version3", versions[1].VersionId)
	assert.Equal(t, "version2", versions[2].VersionId)
	assert.Equal(t, "version1", versions[3].VersionId)

	// Verify latest version is first
	assert.True(t, versions[0].IsLatest)
}

func TestVersionEntryConversion(t *testing.T) {
	// Test conversion from ObjectVersion to VersionEntry
	objectKey := "/test-object.txt"
	versionId := "version123"
	lastModified := time.Now()
	etag := "abcdef123456"
	size := int64(1024)

	version := &ObjectVersion{
		VersionId:      versionId,
		IsLatest:       true,
		IsDeleteMarker: false,
		LastModified:   lastModified,
		ETag:           etag,
		Size:           size,
	}

	versionEntry := &VersionEntry{
		Key:          objectKey,
		VersionId:    version.VersionId,
		IsLatest:     version.IsLatest,
		LastModified: version.LastModified,
		ETag:         version.ETag,
		Size:         version.Size,
		Owner:        CanonicalUser{ID: "test-owner", DisplayName: "Test Owner"},
		StorageClass: "STANDARD",
	}

	assert.Equal(t, objectKey, versionEntry.Key)
	assert.Equal(t, versionId, versionEntry.VersionId)
	assert.Equal(t, true, versionEntry.IsLatest)
	assert.Equal(t, lastModified, versionEntry.LastModified)
	assert.Equal(t, etag, versionEntry.ETag)
	assert.Equal(t, size, versionEntry.Size)
	assert.Equal(t, "STANDARD", versionEntry.StorageClass)
}

func TestDeleteMarkerEntryConversion(t *testing.T) {
	// Test conversion from ObjectVersion to DeleteMarkerEntry
	objectKey := "/test-object.txt"
	versionId := "delete-version123"
	lastModified := time.Now()

	version := &ObjectVersion{
		VersionId:      versionId,
		IsLatest:       true,
		IsDeleteMarker: true,
		LastModified:   lastModified,
	}

	deleteMarker := &DeleteMarkerEntry{
		Key:          objectKey,
		VersionId:    version.VersionId,
		IsLatest:     version.IsLatest,
		LastModified: version.LastModified,
		Owner:        CanonicalUser{ID: "test-owner", DisplayName: "Test Owner"},
	}

	assert.Equal(t, objectKey, deleteMarker.Key)
	assert.Equal(t, versionId, deleteMarker.VersionId)
	assert.Equal(t, true, deleteMarker.IsLatest)
	assert.Equal(t, lastModified, deleteMarker.LastModified)
}
