package schema

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRegistryClient is a mock implementation of RegistryClient for testing
type MockRegistryClient struct {
	subjects map[string]*CachedSubject
	mu       sync.RWMutex
}

// NewMockRegistryClient creates a new mock registry client
func NewMockRegistryClient() *MockRegistryClient {
	return &MockRegistryClient{
		subjects: make(map[string]*CachedSubject),
	}
}

// RegisterMockSchema registers a mock schema for testing
func (m *MockRegistryClient) RegisterMockSchema(id uint32, subject string, format Format, schema string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.subjects[subject] = &CachedSubject{
		Subject:  subject,
		LatestID: id,
		Version:  1,
		Schema:   schema,
		CachedAt: time.Now(),
	}
}

// GetLatestSchema returns the latest schema for a subject
func (m *MockRegistryClient) GetLatestSchema(subject string) (*CachedSubject, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if schema, exists := m.subjects[subject]; exists {
		return schema, nil
	}
	
	return nil, fmt.Errorf("subject %s not found", subject)
}

// GetSchemaByID returns a schema by ID (simplified mock)
func (m *MockRegistryClient) GetSchemaByID(schemaID uint32) (*CachedSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Find schema by ID
	for _, subject := range m.subjects {
		if subject.LatestID == schemaID {
			return &CachedSchema{
				ID:      schemaID,
				Schema:  subject.Schema,
				Subject: subject.Subject,
				Version: subject.Version,
				Format:  detectSchemaFormatFromContent(subject.Schema),
			}, nil
		}
	}
	
	return nil, fmt.Errorf("schema %d not found", schemaID)
}

// Other methods for interface compatibility (not used in tests)
func (m *MockRegistryClient) RegisterSchema(subject, schema string) (uint32, error) {
	return 0, fmt.Errorf("not implemented in mock")
}

func (m *MockRegistryClient) CheckCompatibility(subject, schema string) (bool, error) {
	return true, nil
}

func (m *MockRegistryClient) ListSubjects() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	subjects := make([]string, 0, len(m.subjects))
	for subject := range m.subjects {
		subjects = append(subjects, subject)
	}
	return subjects, nil
}

func (m *MockRegistryClient) ClearCache() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subjects = make(map[string]*CachedSubject)
}

func (m *MockRegistryClient) GetCacheStats() (int, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subjects), len(m.subjects)
}

func TestNewTopicSchemaDetector(t *testing.T) {
	registryClient := &RegistryClient{}
	
	t.Run("Default Configuration", func(t *testing.T) {
		config := TopicSchemaDetectorConfig{}
		detector := NewTopicSchemaDetector(registryClient, config)
		
		assert.NotNil(t, detector)
		assert.Equal(t, 5*time.Minute, detector.config.CacheTTL)
		assert.Len(t, detector.config.SchemaSubjectPatterns, 3)
		assert.Contains(t, detector.config.SchemaSubjectPatterns, "%s")
		assert.Contains(t, detector.config.SchemaSubjectPatterns, "%s-value")
		assert.Contains(t, detector.config.SchemaSubjectPatterns, "%s-key")
	})
	
	t.Run("Custom Configuration", func(t *testing.T) {
		config := TopicSchemaDetectorConfig{
			CacheTTL:                   10 * time.Minute,
			EnableSchemaRegistryLookup: true,
			EnableSeaweedMQLookup:      true,
			SchemaSubjectPatterns:      []string{"%s-custom"},
		}
		detector := NewTopicSchemaDetector(registryClient, config)
		
		assert.Equal(t, 10*time.Minute, detector.config.CacheTTL)
		assert.True(t, detector.config.EnableSchemaRegistryLookup)
		assert.True(t, detector.config.EnableSeaweedMQLookup)
		assert.Len(t, detector.config.SchemaSubjectPatterns, 1)
		assert.Contains(t, detector.config.SchemaSubjectPatterns, "%s-custom")
	})
}

func TestTopicSchemaDetector_MatchesSchemaConventions(t *testing.T) {
	detector := NewTopicSchemaDetector(nil, TopicSchemaDetectorConfig{})
	
	testCases := []struct {
		topicName string
		expected  bool
	}{
		{"user-events-value", true},
		{"user-events-key", true},
		{"schema-user-events", true},
		{"avro-user-events", true},
		{"proto-user-events", true},
		{"json-schema-user-events", true},
		{"regular-topic", false},
		{"user-events", false},
		{"test", false},
		{"", false},
	}
	
	for _, tc := range testCases {
		t.Run(tc.topicName, func(t *testing.T) {
			result := detector.matchesSchemaConventions(tc.topicName)
			assert.Equal(t, tc.expected, result, "Topic: %s", tc.topicName)
		})
	}
}

func TestTopicSchemaDetector_IsSchematizedTopic(t *testing.T) {
	// Create mock registry client
	mockRegistry := NewMockRegistryClient()
	
	// Register a schema for testing
	mockRegistry.RegisterMockSchema(1, "user-events-value", FormatAvro, `{
		"type": "record",
		"name": "UserEvent",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "event", "type": "string"}
		]
	}`)
	
	config := TopicSchemaDetectorConfig{
		EnableSchemaRegistryLookup: true,
		EnableSeaweedMQLookup:      false,
	}
	detector := NewTopicSchemaDetector(mockRegistry, config)
	
	t.Run("Schematized Topic with Registry", func(t *testing.T) {
		result := detector.IsSchematizedTopic("user-events")
		assert.True(t, result)
	})
	
	t.Run("Schematized Topic by Convention", func(t *testing.T) {
		result := detector.IsSchematizedTopic("orders-value")
		assert.True(t, result)
	})
	
	t.Run("Non-Schematized Topic", func(t *testing.T) {
		result := detector.IsSchematizedTopic("regular-topic")
		assert.False(t, result)
	})
	
	t.Run("Caching Works", func(t *testing.T) {
		// First call
		result1 := detector.IsSchematizedTopic("test-topic")
		
		// Second call should use cache
		result2 := detector.IsSchematizedTopic("test-topic")
		
		assert.Equal(t, result1, result2)
		
		// Check cache stats
		stats := detector.GetCacheStats()
		assert.Greater(t, stats["detection_cache_entries"].(int), 0)
	})
}

func TestTopicSchemaDetector_GetSchemaMetadata(t *testing.T) {
	// Create mock registry client
	mockRegistry := NewMockRegistryClient()
	
	// Register a schema for testing
	mockRegistry.RegisterMockSchema(1, "user-events-value", FormatAvro, `{
		"type": "record",
		"name": "UserEvent",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "event", "type": "string"}
		]
	}`)
	
	config := TopicSchemaDetectorConfig{
		EnableSchemaRegistryLookup: true,
	}
	detector := NewTopicSchemaDetector(mockRegistry, config)
	
	t.Run("Get Metadata for Existing Schema", func(t *testing.T) {
		metadata, err := detector.GetSchemaMetadata("user-events")
		require.NoError(t, err)
		
		assert.Equal(t, "user-events", metadata.TopicName)
		assert.Equal(t, uint32(1), metadata.SchemaID)
		assert.Equal(t, FormatAvro, metadata.SchemaFormat)
		assert.Equal(t, "user-events-value", metadata.Subject)
		assert.Equal(t, 1, metadata.Version)
		assert.Contains(t, metadata.SchemaContent, "UserEvent")
		assert.Equal(t, "schema_registry", metadata.Properties["source"])
	})
	
	t.Run("Get Metadata for Non-Existing Schema", func(t *testing.T) {
		_, err := detector.GetSchemaMetadata("non-existent-topic")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no schema metadata found")
	})
	
	t.Run("Metadata Caching", func(t *testing.T) {
		// First call
		metadata1, err := detector.GetSchemaMetadata("user-events")
		require.NoError(t, err)
		
		// Second call should use cache
		metadata2, err := detector.GetSchemaMetadata("user-events")
		require.NoError(t, err)
		
		assert.Equal(t, metadata1.SchemaID, metadata2.SchemaID)
		
		// Check cache stats
		stats := detector.GetCacheStats()
		assert.Greater(t, stats["metadata_cache_entries"].(int), 0)
	})
}

func TestTopicSchemaDetector_CacheManagement(t *testing.T) {
	detector := NewTopicSchemaDetector(nil, TopicSchemaDetectorConfig{})
	
	// Add some data to caches
	detector.detectionCache["topic1"] = true
	detector.metadataCache["topic1"] = &TopicSchemaMetadata{
		TopicName: "topic1",
		SchemaID:  1,
	}
	
	t.Run("Cache Stats", func(t *testing.T) {
		stats := detector.GetCacheStats()
		assert.Equal(t, 1, stats["metadata_cache_entries"])
		assert.Equal(t, 1, stats["detection_cache_entries"])
		assert.Equal(t, float64(300), stats["cache_ttl_seconds"]) // 5 minutes
	})
	
	t.Run("Clear Cache", func(t *testing.T) {
		detector.ClearCache()
		
		stats := detector.GetCacheStats()
		assert.Equal(t, 0, stats["metadata_cache_entries"])
		assert.Equal(t, 0, stats["detection_cache_entries"])
	})
}

func TestTopicSchemaDetector_RefreshTopicMetadata(t *testing.T) {
	// Create mock registry client
	mockRegistry := NewMockRegistryClient()
	mockRegistry.RegisterMockSchema(1, "test-topic-value", FormatAvro, `{"type": "string"}`)
	
	config := TopicSchemaDetectorConfig{
		EnableSchemaRegistryLookup: true,
	}
	detector := NewTopicSchemaDetector(mockRegistry, config)
	
	// Get metadata to populate cache
	metadata1, err := detector.GetSchemaMetadata("test-topic")
	require.NoError(t, err)
	
	// Verify cache is populated
	stats := detector.GetCacheStats()
	assert.Equal(t, 1, stats["metadata_cache_entries"])
	
	// Refresh metadata
	err = detector.RefreshTopicMetadata("test-topic")
	require.NoError(t, err)
	
	// Get metadata again
	metadata2, err := detector.GetSchemaMetadata("test-topic")
	require.NoError(t, err)
	
	// Should be the same data but potentially refreshed
	assert.Equal(t, metadata1.SchemaID, metadata2.SchemaID)
	assert.Equal(t, metadata1.TopicName, metadata2.TopicName)
}

func TestTopicSchemaDetector_HasSchemaInRegistry(t *testing.T) {
	// Create mock registry client
	mockRegistry := NewMockRegistryClient()
	mockRegistry.RegisterMockSchema(1, "events-value", FormatAvro, `{"type": "string"}`)
	mockRegistry.RegisterMockSchema(2, "users-key", FormatAvro, `{"type": "string"}`)
	mockRegistry.RegisterMockSchema(3, "orders", FormatAvro, `{"type": "string"}`)
	
	detector := NewTopicSchemaDetector(mockRegistry, TopicSchemaDetectorConfig{})
	
	testCases := []struct {
		topicName string
		expected  bool
	}{
		{"events", true},   // Should find events-value
		{"users", true},    // Should find users-key
		{"orders", true},   // Should find orders directly
		{"products", false}, // No schema registered
	}
	
	for _, tc := range testCases {
		t.Run(tc.topicName, func(t *testing.T) {
			result := detector.hasSchemaInRegistry(tc.topicName)
			assert.Equal(t, tc.expected, result)
		})
	}
}
