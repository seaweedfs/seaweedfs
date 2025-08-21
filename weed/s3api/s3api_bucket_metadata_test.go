package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/cors"
)

func TestBucketMetadataStruct(t *testing.T) {
	// Test creating empty metadata
	metadata := NewBucketMetadata()
	if !metadata.IsEmpty() {
		t.Error("New metadata should be empty")
	}

	// Test setting tags
	metadata.Tags["Environment"] = "production"
	metadata.Tags["Owner"] = "team-alpha"
	if !metadata.HasTags() {
		t.Error("Metadata should have tags")
	}
	if metadata.IsEmpty() {
		t.Error("Metadata with tags should not be empty")
	}

	// Test setting encryption
	encryption := &s3_pb.EncryptionConfiguration{
		SseAlgorithm: "aws:kms",
		KmsKeyId:     "test-key-id",
	}
	metadata.Encryption = encryption
	if !metadata.HasEncryption() {
		t.Error("Metadata should have encryption")
	}

	// Test setting CORS
	maxAge := 3600
	corsRule := cors.CORSRule{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"*"},
		MaxAgeSeconds:  &maxAge,
	}
	corsConfig := &cors.CORSConfiguration{
		CORSRules: []cors.CORSRule{corsRule},
	}
	metadata.CORS = corsConfig
	if !metadata.HasCORS() {
		t.Error("Metadata should have CORS")
	}

	// Test all flags
	if !metadata.HasTags() || !metadata.HasEncryption() || !metadata.HasCORS() {
		t.Error("All metadata flags should be true")
	}
	if metadata.IsEmpty() {
		t.Error("Metadata with all configurations should not be empty")
	}
}

func TestBucketMetadataUpdatePattern(t *testing.T) {
	// This test demonstrates the update pattern using the function signature
	// (without actually testing the S3ApiServer which would require setup)

	// Simulate what UpdateBucketMetadata would do
	updateFunc := func(metadata *BucketMetadata) error {
		// Add some tags
		metadata.Tags["Project"] = "seaweedfs"
		metadata.Tags["Version"] = "v3.0"

		// Set encryption
		metadata.Encryption = &s3_pb.EncryptionConfiguration{
			SseAlgorithm: "AES256",
		}

		return nil
	}

	// Start with empty metadata
	metadata := NewBucketMetadata()

	// Apply the update
	if err := updateFunc(metadata); err != nil {
		t.Fatalf("Update function failed: %v", err)
	}

	// Verify the results
	if len(metadata.Tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(metadata.Tags))
	}
	if metadata.Tags["Project"] != "seaweedfs" {
		t.Error("Project tag not set correctly")
	}
	if metadata.Encryption == nil || metadata.Encryption.SseAlgorithm != "AES256" {
		t.Error("Encryption not set correctly")
	}
}

func TestBucketMetadataHelperFunctions(t *testing.T) {
	metadata := NewBucketMetadata()

	// Test empty state
	if metadata.HasTags() || metadata.HasCORS() || metadata.HasEncryption() {
		t.Error("Empty metadata should have no configurations")
	}

	// Test adding tags
	metadata.Tags["key1"] = "value1"
	if !metadata.HasTags() {
		t.Error("Should have tags after adding")
	}

	// Test adding CORS
	metadata.CORS = &cors.CORSConfiguration{}
	if !metadata.HasCORS() {
		t.Error("Should have CORS after adding")
	}

	// Test adding encryption
	metadata.Encryption = &s3_pb.EncryptionConfiguration{}
	if !metadata.HasEncryption() {
		t.Error("Should have encryption after adding")
	}

	// Test clearing
	metadata.Tags = make(map[string]string)
	metadata.CORS = nil
	metadata.Encryption = nil

	if metadata.HasTags() || metadata.HasCORS() || metadata.HasEncryption() {
		t.Error("Cleared metadata should have no configurations")
	}
	if !metadata.IsEmpty() {
		t.Error("Cleared metadata should be empty")
	}
}
