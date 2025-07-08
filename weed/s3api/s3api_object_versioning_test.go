package s3api

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Test helper functions
func setupTestS3Server() *S3ApiServer {
	iam := &IdentityAccessManagement{}
	_ = iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{})

	s3a := &S3ApiServer{
		iam:               iam,
		bucketConfigCache: NewBucketConfigCache(5 * time.Minute),
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}
	return s3a
}

// Test Version ID Generation
func TestGenerateVersionId(t *testing.T) {
	// Test version ID generation (package-level function)
	versionId1 := generateVersionId()
	versionId2 := generateVersionId()

	// Verify version IDs are non-empty
	if versionId1 == "" {
		t.Error("Generated version ID should not be empty")
	}
	if versionId2 == "" {
		t.Error("Generated version ID should not be empty")
	}

	// Verify version IDs are unique
	if versionId1 == versionId2 {
		t.Error("Generated version IDs should be unique")
	}

	// Verify version ID format (should be 32 character hex string)
	if len(versionId1) != 32 {
		t.Errorf("Version ID should be 32 characters, got %d", len(versionId1))
	}

	// Verify it's valid hex
	if _, err := hex.DecodeString(versionId1); err != nil {
		t.Errorf("Version ID should be valid hex string: %v", err)
	}
}

// Test Bucket Config Cache
func TestBucketConfigCache(t *testing.T) {
	cache := NewBucketConfigCache(5 * time.Minute)
	bucketName := "test-cache-bucket"

	// Test cache miss
	config, found := cache.Get(bucketName)
	if found {
		t.Error("Expected cache miss for non-existent bucket")
	}
	if config != nil {
		t.Error("Config should be nil for cache miss")
	}

	// Test cache set and get
	testConfig := &BucketConfig{
		Name:         bucketName,
		Versioning:   "Enabled",
		LastModified: time.Now(),
	}

	cache.Set(bucketName, testConfig)

	retrievedConfig, found := cache.Get(bucketName)
	if !found {
		t.Fatal("Expected to find config in cache")
	}
	if retrievedConfig == nil {
		t.Fatal("Expected to retrieve config from cache")
	}

	if retrievedConfig.Name != bucketName {
		t.Errorf("Expected bucket name '%s', got '%s'", bucketName, retrievedConfig.Name)
	}

	if retrievedConfig.Versioning != "Enabled" {
		t.Errorf("Expected versioning status 'Enabled', got '%s'", retrievedConfig.Versioning)
	}

	// Test cache removal
	cache.Remove(bucketName)

	config, found = cache.Get(bucketName)
	if found {
		t.Error("Expected cache miss after removal")
	}
}

// Test Cache TTL Expiration
func TestBucketConfigCacheTTL(t *testing.T) {
	// Create cache with short TTL for testing
	cache := NewBucketConfigCache(100 * time.Millisecond)

	bucketName := "test-ttl-bucket"
	testConfig := &BucketConfig{
		Name:         bucketName,
		LastModified: time.Now(),
	}

	// Set config
	cache.Set(bucketName, testConfig)

	// Verify it exists
	config, found := cache.Get(bucketName)
	if !found || config == nil {
		t.Fatal("Expected to retrieve config from cache")
	}

	// Wait for TTL expiration
	time.Sleep(150 * time.Millisecond)

	// Verify it's expired
	config, found = cache.Get(bucketName)
	if found {
		t.Error("Expected cache miss after TTL expiration")
	}
}

// Test Versioned Object Directory Generation
func TestVersionedObjectDirectories(t *testing.T) {
	s3a := setupTestS3Server()
	bucket := "test-bucket"
	object := "/test/object.txt"

	// Test versioned object directory
	versionedDir := s3a.getVersionedObjectDir(bucket, object)
	expectedDir := "/buckets/test-bucket/test/object.txt.versions"

	if versionedDir != expectedDir {
		t.Errorf("Expected versioned directory '%s', got '%s'", expectedDir, versionedDir)
	}

	// Test with root level object
	rootObject := "/root.txt"
	versionedDir = s3a.getVersionedObjectDir(bucket, rootObject)
	expectedDir = "/buckets/test-bucket/root.txt.versions"

	if versionedDir != expectedDir {
		t.Errorf("Expected versioned directory '%s', got '%s'", expectedDir, versionedDir)
	}
}

// Test Version File Names
func TestVersionFileNames(t *testing.T) {
	s3a := setupTestS3Server()
	versionId := "abc123def456"

	// Test version file name generation
	fileName := s3a.getVersionFileName(versionId)

	if fileName != versionId {
		t.Errorf("Expected version file name '%s', got '%s'", versionId, fileName)
	}
}

// Test Object Version Metadata
func TestObjectVersionMetadata(t *testing.T) {
	versionId := "version123"
	isLatest := true

	// Test metadata creation
	metadata := make(map[string][]byte)
	metadata[s3_constants.ExtVersionIdKey] = []byte(versionId)
	metadata[s3_constants.ExtIsLatestKey] = []byte(fmt.Sprintf("%t", isLatest))

	if metadata[s3_constants.ExtVersionIdKey] == nil {
		t.Error("Version ID should be set in metadata")
	}

	if string(metadata[s3_constants.ExtVersionIdKey]) != versionId {
		t.Errorf("Expected version ID '%s', got '%s'", versionId, string(metadata[s3_constants.ExtVersionIdKey]))
	}

	if metadata[s3_constants.ExtIsLatestKey] == nil {
		t.Error("IsLatest should be set in metadata")
	}

	if string(metadata[s3_constants.ExtIsLatestKey]) != "true" {
		t.Error("IsLatest should be 'true'")
	}

	// Test with non-latest version
	metadata[s3_constants.ExtIsLatestKey] = []byte("false")
	if string(metadata[s3_constants.ExtIsLatestKey]) != "false" {
		t.Error("IsLatest should be 'false' for non-latest version")
	}
}

// Test Versioning XML Responses
func TestVersioningXMLResponses(t *testing.T) {
	// Test GetBucketVersioning XML response
	versioningConfig := &VersioningConfiguration{
		Status: "Enabled",
	}

	xmlData, err := xml.Marshal(versioningConfig)
	if err != nil {
		t.Fatalf("Failed to marshal versioning configuration: %v", err)
	}

	// Verify XML contains expected elements
	xmlString := string(xmlData)
	if !strings.Contains(xmlString, "<Status>Enabled</Status>") {
		t.Error("XML should contain versioning status")
	}

	// Test suspended versioning
	versioningConfig.Status = "Suspended"
	xmlData, err = xml.Marshal(versioningConfig)
	if err != nil {
		t.Fatalf("Failed to marshal suspended versioning configuration: %v", err)
	}

	xmlString = string(xmlData)
	if !strings.Contains(xmlString, "<Status>Suspended</Status>") {
		t.Error("XML should contain suspended status")
	}
}

// Test Concurrent Access to Bucket Config Cache
func TestBucketConfigCacheConcurrency(t *testing.T) {
	cache := NewBucketConfigCache(5 * time.Minute)
	bucketName := "test-concurrent-bucket"

	// Run concurrent operations
	done := make(chan bool, 10)

	// Start multiple goroutines
	for i := 0; i < 10; i++ {
		go func(id int) {
			config := &BucketConfig{
				Name:         fmt.Sprintf("%s-%d", bucketName, id),
				LastModified: time.Now(),
			}

			// Set config
			cache.Set(config.Name, config)

			// Get config
			retrieved, found := cache.Get(config.Name)
			if !found || retrieved == nil {
				t.Errorf("Failed to retrieve config for goroutine %d", id)
			}

			// Remove config
			cache.Remove(config.Name)

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// Test Cache Clear functionality
func TestBucketConfigCacheClear(t *testing.T) {
	cache := NewBucketConfigCache(5 * time.Minute)

	// Add multiple configs
	for i := 0; i < 5; i++ {
		config := &BucketConfig{
			Name:         fmt.Sprintf("bucket-%d", i),
			LastModified: time.Now(),
		}
		cache.Set(config.Name, config)
	}

	// Verify configs exist
	for i := 0; i < 5; i++ {
		bucketName := fmt.Sprintf("bucket-%d", i)
		_, found := cache.Get(bucketName)
		if !found {
			t.Errorf("Expected to find config for bucket %s", bucketName)
		}
	}

	// Clear cache
	cache.Clear()

	// Verify configs are gone
	for i := 0; i < 5; i++ {
		bucketName := fmt.Sprintf("bucket-%d", i)
		_, found := cache.Get(bucketName)
		if found {
			t.Errorf("Expected cache to be cleared for bucket %s", bucketName)
		}
	}
}

// Test Version ID Uniqueness Over Time
func TestVersionIdUniquenessOverTime(t *testing.T) {
	versionIds := make(map[string]bool)

	// Generate multiple version IDs
	for i := 0; i < 1000; i++ {
		versionId := generateVersionId()
		if versionIds[versionId] {
			t.Errorf("Duplicate version ID generated: %s", versionId)
		}
		versionIds[versionId] = true
	}
}

// Test Version ID Format Consistency
func TestVersionIdFormatConsistency(t *testing.T) {
	for i := 0; i < 100; i++ {
		versionId := generateVersionId()

		// Check length
		if len(versionId) != 32 {
			t.Errorf("Version ID should be 32 characters, got %d", len(versionId))
		}

		// Check hex format
		if _, err := hex.DecodeString(versionId); err != nil {
			t.Errorf("Version ID should be valid hex string: %v", err)
		}

		// Check character set (should only contain hex characters)
		for _, char := range versionId {
			if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
				t.Errorf("Version ID contains invalid character: %c", char)
			}
		}
	}
}

// Test Bucket Config Structure
func TestBucketConfigStructure(t *testing.T) {
	config := &BucketConfig{
		Name:         "test-bucket",
		Versioning:   "Enabled",
		Ownership:    "BucketOwnerPreferred",
		Owner:        "test-owner",
		LastModified: time.Now(),
	}

	// Verify all fields are set
	if config.Name != "test-bucket" {
		t.Errorf("Expected bucket name 'test-bucket', got '%s'", config.Name)
	}

	if config.Versioning != "Enabled" {
		t.Errorf("Expected versioning 'Enabled', got '%s'", config.Versioning)
	}

	if config.Ownership != "BucketOwnerPreferred" {
		t.Errorf("Expected ownership 'BucketOwnerPreferred', got '%s'", config.Ownership)
	}

	if config.Owner != "test-owner" {
		t.Errorf("Expected owner 'test-owner', got '%s'", config.Owner)
	}

	if config.LastModified.IsZero() {
		t.Error("LastModified should be set")
	}
}

// Test XML Versioning Configuration Structure
func TestVersioningConfigurationXML(t *testing.T) {
	// Test various versioning configurations
	testCases := []struct {
		status    VersioningStatus
		mfaDelete string
		expected  []string
	}{
		{
			status:    VersioningStatus("Enabled"),
			mfaDelete: "",
			expected:  []string{"<Status>Enabled</Status>"},
		},
		{
			status:    VersioningStatus("Suspended"),
			mfaDelete: "",
			expected:  []string{"<Status>Suspended</Status>"},
		},
	}

	for _, tc := range testCases {
		config := &VersioningConfiguration{
			Status: tc.status,
		}

		xmlData, err := xml.Marshal(config)
		if err != nil {
			t.Fatalf("Failed to marshal versioning configuration: %v", err)
		}

		xmlString := string(xmlData)

		for _, expected := range tc.expected {
			if !strings.Contains(xmlString, expected) {
				t.Errorf("XML should contain '%s', got: %s", expected, xmlString)
			}
		}
	}
}

// Test S3 Constants for Versioning
func TestS3VersioningConstants(t *testing.T) {
	// Verify that required constants are defined
	if s3_constants.ExtVersionIdKey == "" {
		t.Error("ExtVersionIdKey should be defined")
	}

	if s3_constants.ExtIsLatestKey == "" {
		t.Error("ExtIsLatestKey should be defined")
	}

	if s3_constants.ExtVersioningKey == "" {
		t.Error("ExtVersioningKey should be defined")
	}

	// Verify constants have reasonable values
	expectedConstants := map[string]string{
		"ExtVersionIdKey":  s3_constants.ExtVersionIdKey,
		"ExtIsLatestKey":   s3_constants.ExtIsLatestKey,
		"ExtVersioningKey": s3_constants.ExtVersioningKey,
	}

	for name, value := range expectedConstants {
		if value == "" {
			t.Errorf("Constant %s should not be empty", name)
		}
		if len(value) < 3 {
			t.Errorf("Constant %s should be at least 3 characters, got '%s'", name, value)
		}
	}
}

// Benchmark version ID generation
func BenchmarkGenerateVersionId(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = generateVersionId()
	}
}

// Benchmark bucket config cache operations
func BenchmarkBucketConfigCache(b *testing.B) {
	cache := NewBucketConfigCache(5 * time.Minute)
	bucketName := "benchmark-bucket"
	config := &BucketConfig{
		Name:         bucketName,
		LastModified: time.Now(),
	}

	b.ResetTimer()

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache.Set(fmt.Sprintf("%s-%d", bucketName, i), config)
		}
	})

	// Pre-populate cache for Get benchmark
	cache.Set(bucketName, config)

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = cache.Get(bucketName)
		}
	})

	b.Run("Remove", func(b *testing.B) {
		// Pre-populate for removal benchmark
		for i := 0; i < b.N; i++ {
			cache.Set(fmt.Sprintf("%s-%d", bucketName, i), config)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Remove(fmt.Sprintf("%s-%d", bucketName, i))
		}
	})
}

func TestVersionedDirectoryCreation(t *testing.T) {
	// Mock S3ApiServer
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	bucket := "test-bucket"
	object := "test/object.txt"

	// Test getVersionedObjectDir
	expectedDir := "/buckets/test-bucket/test/object.txt.versions"
	actualDir := s3a.getVersionedObjectDir(bucket, object)
	if actualDir != expectedDir {
		t.Errorf("Expected directory %s, got %s", expectedDir, actualDir)
	}

	// Test with root object
	object = "root.txt"
	expectedDir = "/buckets/test-bucket/root.txt.versions"
	actualDir = s3a.getVersionedObjectDir(bucket, object)
	if actualDir != expectedDir {
		t.Errorf("Expected directory %s, got %s", expectedDir, actualDir)
	}
}

func TestVersioningMetadataHandling(t *testing.T) {
	// Test that versioning metadata is properly handled
	object := "test-object.txt"
	versionId := "test-version-id-123456789012345678901234"

	// Create a test entry
	entry := &filer_pb.Entry{
		Name: object,
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
			Mtime:    time.Now().Unix(),
		},
	}

	// Test that we can add versioning metadata
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	entry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
	entry.Extended[s3_constants.ExtIsLatestKey] = []byte("true")

	// Verify metadata was added correctly
	versionIdBytes, hasVersionId := entry.Extended[s3_constants.ExtVersionIdKey]
	if !hasVersionId {
		t.Error("Expected version ID in entry extended attributes")
	} else if string(versionIdBytes) != versionId {
		t.Errorf("Expected version ID %s, got %s", versionId, string(versionIdBytes))
	}

	isLatestBytes, hasIsLatest := entry.Extended[s3_constants.ExtIsLatestKey]
	if !hasIsLatest {
		t.Error("Expected isLatest flag in entry extended attributes")
	} else if string(isLatestBytes) != "true" {
		t.Errorf("Expected isLatest to be true, got %s", string(isLatestBytes))
	}
}

func TestVersioningDirectoryPaths(t *testing.T) {
	// Test directory path calculations for versioning
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	tests := []struct {
		bucket       string
		object       string
		expectedPath string
	}{
		{"test-bucket", "test-object.txt", "/buckets/test-bucket/test-object.txt.versions"},
		{"my-bucket", "/path/to/file.jpg", "/buckets/my-bucket/path/to/file.jpg.versions"},
		{"bucket", "root.txt", "/buckets/bucket/root.txt.versions"},
		{"test", "/deep/nested/path/file.dat", "/buckets/test/deep/nested/path/file.dat.versions"},
	}

	for _, test := range tests {
		actualPath := s3a.getVersionedObjectDir(test.bucket, test.object)
		if actualPath != test.expectedPath {
			t.Errorf("For bucket=%s, object=%s: expected path %s, got %s",
				test.bucket, test.object, test.expectedPath, actualPath)
		}
	}
}
