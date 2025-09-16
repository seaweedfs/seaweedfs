package schema

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// TopicSchemaMetadata holds schema information for a topic
type TopicSchemaMetadata struct {
	TopicName     string            `json:"topic_name"`
	SchemaID      uint32            `json:"schema_id"`
	SchemaFormat  Format            `json:"schema_format"`
	Subject       string            `json:"subject"`
	Version       int               `json:"version"`
	SchemaContent string            `json:"schema_content"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Properties    map[string]string `json:"properties,omitempty"`
}

// RegistryClientInterface defines the interface for registry client operations
type RegistryClientInterface interface {
	GetLatestSchema(subject string) (*CachedSubject, error)
	GetSchemaByID(schemaID uint32) (*CachedSchema, error)
	RegisterSchema(subject, schema string) (uint32, error)
	CheckCompatibility(subject, schema string) (bool, error)
	ListSubjects() ([]string, error)
	ClearCache()
	GetCacheStats() (int, int)
}

// TopicSchemaDetector provides schema detection and metadata management for topics
type TopicSchemaDetector struct {
	registryClient RegistryClientInterface
	
	// Cache for topic schema metadata
	metadataCache map[string]*TopicSchemaMetadata
	cacheMu       sync.RWMutex
	
	// Cache for topic schema detection results
	detectionCache map[string]bool
	detectionMu    sync.RWMutex
	
	// Configuration
	config TopicSchemaDetectorConfig
}

// TopicSchemaDetectorConfig holds configuration for the detector
type TopicSchemaDetectorConfig struct {
	// CacheTTL is how long to cache detection results
	CacheTTL time.Duration
	
	// EnableSchemaRegistryLookup enables checking Schema Registry for topic schemas
	EnableSchemaRegistryLookup bool
	
	// EnableSeaweedMQLookup enables checking SeaweedMQ metadata for schemas
	EnableSeaweedMQLookup bool
	
	// SchemaSubjectPatterns defines patterns for mapping topics to schema subjects
	SchemaSubjectPatterns []string
}

// NewTopicSchemaDetector creates a new topic schema detector
func NewTopicSchemaDetector(registryClient RegistryClientInterface, config TopicSchemaDetectorConfig) *TopicSchemaDetector {
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute
	}
	
	if len(config.SchemaSubjectPatterns) == 0 {
		// Default patterns following Confluent conventions
		config.SchemaSubjectPatterns = []string{
			"%s",        // Direct topic name
			"%s-value",  // Topic name with -value suffix
			"%s-key",    // Topic name with -key suffix
		}
	}
	
	return &TopicSchemaDetector{
		registryClient: registryClient,
		metadataCache:  make(map[string]*TopicSchemaMetadata),
		detectionCache: make(map[string]bool),
		config:         config,
	}
}

// IsSchematizedTopic checks if a topic uses schema management
func (tsd *TopicSchemaDetector) IsSchematizedTopic(topicName string) bool {
	// Check cache first
	tsd.detectionMu.RLock()
	if cached, exists := tsd.detectionCache[topicName]; exists {
		tsd.detectionMu.RUnlock()
		return cached
	}
	tsd.detectionMu.RUnlock()
	
	// Perform detection
	isSchematized := tsd.detectSchematizedTopic(topicName)
	
	// Cache result
	tsd.detectionMu.Lock()
	tsd.detectionCache[topicName] = isSchematized
	tsd.detectionMu.Unlock()
	
	return isSchematized
}

// detectSchematizedTopic performs the actual schema detection
func (tsd *TopicSchemaDetector) detectSchematizedTopic(topicName string) bool {
	// 1. Check Schema Registry if enabled
	if tsd.config.EnableSchemaRegistryLookup && tsd.registryClient != nil {
		if tsd.hasSchemaInRegistry(topicName) {
			return true
		}
	}
	
	// 2. Check SeaweedMQ metadata if enabled
	if tsd.config.EnableSeaweedMQLookup {
		if tsd.hasSchemaInSeaweedMQ(topicName) {
			return true
		}
	}
	
	// 3. Check naming conventions
	if tsd.matchesSchemaConventions(topicName) {
		return true
	}
	
	return false
}

// hasSchemaInRegistry checks if topic has schema in Schema Registry
func (tsd *TopicSchemaDetector) hasSchemaInRegistry(topicName string) bool {
	for _, pattern := range tsd.config.SchemaSubjectPatterns {
		subject := fmt.Sprintf(pattern, topicName)
		
		// Try to get latest schema for this subject
		_, err := tsd.registryClient.GetLatestSchema(subject)
		if err == nil {
			return true
		}
	}
	
	return false
}

// hasSchemaInSeaweedMQ checks if topic has schema metadata in SeaweedMQ
func (tsd *TopicSchemaDetector) hasSchemaInSeaweedMQ(topicName string) bool {
	// TODO: Implement SeaweedMQ integration
	// This would check SeaweedMQ's topic metadata for schema information
	return false
}

// matchesSchemaConventions checks if topic name matches schema naming conventions
func (tsd *TopicSchemaDetector) matchesSchemaConventions(topicName string) bool {
	// Check for common schema topic naming patterns
	if len(topicName) > 6 && topicName[len(topicName)-6:] == "-value" {
		return true
	}
	if len(topicName) > 4 && topicName[len(topicName)-4:] == "-key" {
		return true
	}
	
	// Check for other common patterns
	schemaPatterns := []string{
		"schema-",
		"avro-",
		"proto-",
		"json-schema-",
	}
	
	for _, pattern := range schemaPatterns {
		if len(topicName) >= len(pattern) && topicName[:len(pattern)] == pattern {
			return true
		}
	}
	
	return false
}

// GetSchemaMetadata retrieves schema metadata for a topic
func (tsd *TopicSchemaDetector) GetSchemaMetadata(topicName string) (*TopicSchemaMetadata, error) {
	// Check cache first
	tsd.cacheMu.RLock()
	if cached, exists := tsd.metadataCache[topicName]; exists {
		tsd.cacheMu.RUnlock()
		return cached, nil
	}
	tsd.cacheMu.RUnlock()
	
	// Fetch metadata
	metadata, err := tsd.fetchSchemaMetadata(topicName)
	if err != nil {
		return nil, err
	}
	
	// Cache metadata
	tsd.cacheMu.Lock()
	tsd.metadataCache[topicName] = metadata
	tsd.cacheMu.Unlock()
	
	return metadata, nil
}

// fetchSchemaMetadata fetches schema metadata from various sources
func (tsd *TopicSchemaDetector) fetchSchemaMetadata(topicName string) (*TopicSchemaMetadata, error) {
	// Try Schema Registry first
	if tsd.config.EnableSchemaRegistryLookup && tsd.registryClient != nil {
		metadata, err := tsd.fetchFromRegistry(topicName)
		if err == nil {
			return metadata, nil
		}
	}
	
	// Try SeaweedMQ metadata
	if tsd.config.EnableSeaweedMQLookup {
		metadata, err := tsd.fetchFromSeaweedMQ(topicName)
		if err == nil {
			return metadata, nil
		}
	}
	
	return nil, fmt.Errorf("no schema metadata found for topic %s", topicName)
}

// fetchFromRegistry fetches schema metadata from Schema Registry
func (tsd *TopicSchemaDetector) fetchFromRegistry(topicName string) (*TopicSchemaMetadata, error) {
	for _, pattern := range tsd.config.SchemaSubjectPatterns {
		subject := fmt.Sprintf(pattern, topicName)
		
		schema, err := tsd.registryClient.GetLatestSchema(subject)
		if err != nil {
			continue // Try next pattern
		}
		
		// Found schema, build metadata
		// Detect format from schema content
		format := detectSchemaFormatFromContent(schema.Schema)
		
		metadata := &TopicSchemaMetadata{
			TopicName:     topicName,
			SchemaID:      schema.LatestID,
			SchemaFormat:  format,
			Subject:       subject,
			Version:       schema.Version,
			SchemaContent: schema.Schema,
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			Properties: map[string]string{
				"source": "schema_registry",
			},
		}
		
		return metadata, nil
	}
	
	return nil, fmt.Errorf("no schema found in registry for topic %s", topicName)
}

// fetchFromSeaweedMQ fetches schema metadata from SeaweedMQ
func (tsd *TopicSchemaDetector) fetchFromSeaweedMQ(topicName string) (*TopicSchemaMetadata, error) {
	// TODO: Implement SeaweedMQ integration
	return nil, fmt.Errorf("SeaweedMQ schema metadata not implemented")
}

// ClearCache clears all cached data
func (tsd *TopicSchemaDetector) ClearCache() {
	tsd.cacheMu.Lock()
	tsd.metadataCache = make(map[string]*TopicSchemaMetadata)
	tsd.cacheMu.Unlock()
	
	tsd.detectionMu.Lock()
	tsd.detectionCache = make(map[string]bool)
	tsd.detectionMu.Unlock()
}

// GetCacheStats returns cache statistics
func (tsd *TopicSchemaDetector) GetCacheStats() map[string]interface{} {
	tsd.cacheMu.RLock()
	metadataCount := len(tsd.metadataCache)
	tsd.cacheMu.RUnlock()
	
	tsd.detectionMu.RLock()
	detectionCount := len(tsd.detectionCache)
	tsd.detectionMu.RUnlock()
	
	return map[string]interface{}{
		"metadata_cache_entries":  metadataCount,
		"detection_cache_entries": detectionCount,
		"cache_ttl_seconds":       tsd.config.CacheTTL.Seconds(),
	}
}

// RefreshTopicMetadata forces a refresh of metadata for a specific topic
func (tsd *TopicSchemaDetector) RefreshTopicMetadata(topicName string) error {
	// Remove from caches
	tsd.cacheMu.Lock()
	delete(tsd.metadataCache, topicName)
	tsd.cacheMu.Unlock()
	
	tsd.detectionMu.Lock()
	delete(tsd.detectionCache, topicName)
	tsd.detectionMu.Unlock()
	
	// Re-fetch metadata
	_, err := tsd.GetSchemaMetadata(topicName)
	return err
}

// detectSchemaFormatFromContent detects schema format from content
func detectSchemaFormatFromContent(schema string) Format {
	// Try to parse as JSON first (Avro schemas are JSON)
	var jsonObj interface{}
	if err := json.Unmarshal([]byte(schema), &jsonObj); err == nil {
		// Check for Avro-specific fields
		if schemaMap, ok := jsonObj.(map[string]interface{}); ok {
			if schemaType, exists := schemaMap["type"]; exists {
				if typeStr, ok := schemaType.(string); ok {
					// Common Avro types
					avroTypes := []string{"record", "enum", "array", "map", "union", "fixed"}
					for _, avroType := range avroTypes {
						if typeStr == avroType {
							return FormatAvro
						}
					}
					// Common JSON Schema types (that are not Avro types)
					jsonSchemaTypes := []string{"object", "number", "integer", "boolean", "null"}
					for _, jsonSchemaType := range jsonSchemaTypes {
						if typeStr == jsonSchemaType {
							return FormatJSONSchema
						}
					}
				}
			}
			// Check for JSON Schema indicators
			if _, exists := schemaMap["$schema"]; exists {
				return FormatJSONSchema
			}
			// Check for JSON Schema properties field
			if _, exists := schemaMap["properties"]; exists {
				return FormatJSONSchema
			}
		}
		// Default JSON-based schema to Avro
		return FormatAvro
	}

	// Check for Protobuf (typically not JSON)
	return FormatProtobuf
}
