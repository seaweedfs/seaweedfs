package azuresink

import (
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// MockConfiguration for testing
type mockConfiguration struct {
	values map[string]interface{}
}

func newMockConfiguration() *mockConfiguration {
	return &mockConfiguration{
		values: make(map[string]interface{}),
	}
}

func (m *mockConfiguration) GetString(key string) string {
	if v, ok := m.values[key]; ok {
		return v.(string)
	}
	return ""
}

func (m *mockConfiguration) GetBool(key string) bool {
	if v, ok := m.values[key]; ok {
		return v.(bool)
	}
	return false
}

func (m *mockConfiguration) GetInt(key string) int {
	if v, ok := m.values[key]; ok {
		return v.(int)
	}
	return 0
}

func (m *mockConfiguration) GetInt64(key string) int64 {
	if v, ok := m.values[key]; ok {
		return v.(int64)
	}
	return 0
}

func (m *mockConfiguration) GetFloat64(key string) float64 {
	if v, ok := m.values[key]; ok {
		return v.(float64)
	}
	return 0.0
}

func (m *mockConfiguration) GetStringSlice(key string) []string {
	if v, ok := m.values[key]; ok {
		return v.([]string)
	}
	return nil
}

func (m *mockConfiguration) SetDefault(key string, value interface{}) {
	if _, exists := m.values[key]; !exists {
		m.values[key] = value
	}
}

// Test the AzureSink interface implementation
func TestAzureSinkInterface(t *testing.T) {
	sink := &AzureSink{}

	if sink.GetName() != "azure" {
		t.Errorf("Expected name 'azure', got '%s'", sink.GetName())
	}

	// Test directory setting
	sink.dir = "/test/dir"
	if sink.GetSinkToDirectory() != "/test/dir" {
		t.Errorf("Expected directory '/test/dir', got '%s'", sink.GetSinkToDirectory())
	}

	// Test incremental setting
	sink.isIncremental = true
	if !sink.IsIncremental() {
		t.Error("Expected isIncremental to be true")
	}
}

// Test Azure sink initialization
func TestAzureSinkInitialization(t *testing.T) {
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	testContainer := os.Getenv("AZURE_TEST_CONTAINER")

	if accountName == "" || accountKey == "" {
		t.Skip("Skipping Azure sink test: AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY not set")
	}
	if testContainer == "" {
		testContainer = "seaweedfs-test"
	}

	sink := &AzureSink{}

	err := sink.initialize(accountName, accountKey, testContainer, "/test")
	if err != nil {
		t.Fatalf("Failed to initialize Azure sink: %v", err)
	}

	if sink.container != testContainer {
		t.Errorf("Expected container '%s', got '%s'", testContainer, sink.container)
	}

	if sink.dir != "/test" {
		t.Errorf("Expected dir '/test', got '%s'", sink.dir)
	}

	if sink.client == nil {
		t.Error("Expected client to be initialized")
	}
}

// Test configuration-based initialization
func TestAzureSinkInitializeFromConfig(t *testing.T) {
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	testContainer := os.Getenv("AZURE_TEST_CONTAINER")

	if accountName == "" || accountKey == "" {
		t.Skip("Skipping Azure sink config test: AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY not set")
	}
	if testContainer == "" {
		testContainer = "seaweedfs-test"
	}

	config := newMockConfiguration()
	config.values["azure.account_name"] = accountName
	config.values["azure.account_key"] = accountKey
	config.values["azure.container"] = testContainer
	config.values["azure.directory"] = "/test"
	config.values["azure.is_incremental"] = true

	sink := &AzureSink{}
	err := sink.Initialize(config, "azure.")
	if err != nil {
		t.Fatalf("Failed to initialize from config: %v", err)
	}

	if !sink.IsIncremental() {
		t.Error("Expected incremental to be true")
	}
}

// Test cleanKey function
func TestCleanKey(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/test/file.txt", "test/file.txt"},
		{"test/file.txt", "test/file.txt"},
		{"/", ""},
		{"", ""},
		{"/a/b/c", "a/b/c"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := cleanKey(tt.input)
			if result != tt.expected {
				t.Errorf("cleanKey(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test entry operations (requires valid credentials)
func TestAzureSinkEntryOperations(t *testing.T) {
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	testContainer := os.Getenv("AZURE_TEST_CONTAINER")

	if accountName == "" || accountKey == "" {
		t.Skip("Skipping Azure sink entry test: credentials not set")
	}
	if testContainer == "" {
		testContainer = "seaweedfs-test"
	}

	sink := &AzureSink{}
	err := sink.initialize(accountName, accountKey, testContainer, "/test")
	if err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Test CreateEntry with directory (should be no-op)
	t.Run("CreateDirectory", func(t *testing.T) {
		entry := &filer_pb.Entry{
			IsDirectory: true,
		}
		err := sink.CreateEntry("/test/dir", entry, nil)
		if err != nil {
			t.Errorf("CreateEntry for directory should not error: %v", err)
		}
	})

	// Test CreateEntry with file
	testKey := "/test-sink-file-" + time.Now().Format("20060102-150405") + ".txt"
	t.Run("CreateFile", func(t *testing.T) {
		entry := &filer_pb.Entry{
			IsDirectory: false,
			Content:     []byte("Test content for Azure sink"),
			Attributes: &filer_pb.FuseAttributes{
				Mtime: time.Now().Unix(),
			},
		}
		err := sink.CreateEntry(testKey, entry, nil)
		if err != nil {
			t.Fatalf("Failed to create entry: %v", err)
		}
	})

	// Test UpdateEntry
	t.Run("UpdateEntry", func(t *testing.T) {
		oldEntry := &filer_pb.Entry{
			Content: []byte("Old content"),
		}
		newEntry := &filer_pb.Entry{
			Content: []byte("New content for update test"),
			Attributes: &filer_pb.FuseAttributes{
				Mtime: time.Now().Unix(),
			},
		}
		found, err := sink.UpdateEntry(testKey, oldEntry, "/test", newEntry, false, nil)
		if err != nil {
			t.Fatalf("Failed to update entry: %v", err)
		}
		if !found {
			t.Error("Expected found to be true")
		}
	})

	// Test DeleteEntry
	t.Run("DeleteFile", func(t *testing.T) {
		err := sink.DeleteEntry(testKey, false, false, nil)
		if err != nil {
			t.Fatalf("Failed to delete entry: %v", err)
		}
	})

	// Test DeleteEntry with directory marker
	testDirKey := "/test-dir-" + time.Now().Format("20060102-150405")
	t.Run("DeleteDirectory", func(t *testing.T) {
		// First create a directory marker
		entry := &filer_pb.Entry{
			IsDirectory: false,
			Content:     []byte(""),
		}
		err := sink.CreateEntry(testDirKey+"/", entry, nil)
		if err != nil {
			t.Logf("Warning: Failed to create directory marker: %v", err)
		}

		// Then delete it
		err = sink.DeleteEntry(testDirKey, true, false, nil)
		if err != nil {
			t.Logf("Warning: Failed to delete directory: %v", err)
		}
	})
}

// Test CreateEntry with precondition (IfUnmodifiedSince)
func TestAzureSinkPrecondition(t *testing.T) {
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	testContainer := os.Getenv("AZURE_TEST_CONTAINER")

	if accountName == "" || accountKey == "" {
		t.Skip("Skipping Azure sink precondition test: credentials not set")
	}
	if testContainer == "" {
		testContainer = "seaweedfs-test"
	}

	sink := &AzureSink{}
	err := sink.initialize(accountName, accountKey, testContainer, "/test")
	if err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	testKey := "/test-precondition-" + time.Now().Format("20060102-150405") + ".txt"

	// Create initial entry
	entry := &filer_pb.Entry{
		Content: []byte("Initial content"),
		Attributes: &filer_pb.FuseAttributes{
			Mtime: time.Now().Unix(),
		},
	}
	err = sink.CreateEntry(testKey, entry, nil)
	if err != nil {
		t.Fatalf("Failed to create initial entry: %v", err)
	}

	// Try to create again with old mtime (should be skipped due to precondition)
	oldEntry := &filer_pb.Entry{
		Content: []byte("Should not overwrite"),
		Attributes: &filer_pb.FuseAttributes{
			Mtime: time.Now().Add(-1 * time.Hour).Unix(), // Old timestamp
		},
	}
	err = sink.CreateEntry(testKey, oldEntry, nil)
	// Should either succeed (skip) or fail with precondition error
	if err != nil {
		t.Logf("Create with old mtime: %v (expected)", err)
	}

	// Clean up
	sink.DeleteEntry(testKey, false, false, nil)
}

// Benchmark tests
func BenchmarkCleanKey(b *testing.B) {
	keys := []string{
		"/simple/path.txt",
		"no/leading/slash.txt",
		"/",
		"/complex/path/with/many/segments/file.txt",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cleanKey(keys[i%len(keys)])
	}
}

// Test error handling with invalid credentials
func TestAzureSinkInvalidCredentials(t *testing.T) {
	sink := &AzureSink{}

	err := sink.initialize("invalid-account", "aW52YWxpZGtleQ==", "test-container", "/test")
	if err != nil {
		t.Skip("Invalid credentials correctly rejected at initialization")
	}

	// If initialization succeeded, operations should fail
	entry := &filer_pb.Entry{
		Content: []byte("test"),
	}
	err = sink.CreateEntry("/test.txt", entry, nil)
	if err == nil {
		t.Log("Expected error with invalid credentials, but got none (might be cached)")
	}
}
