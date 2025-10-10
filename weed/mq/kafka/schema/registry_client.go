package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// RegistryClient provides access to a Confluent Schema Registry
type RegistryClient struct {
	baseURL    string
	httpClient *http.Client

	// Caching
	schemaCache      map[uint32]*CachedSchema  // schema ID -> schema
	subjectCache     map[string]*CachedSubject // subject -> latest version info
	negativeCache    map[string]time.Time      // subject -> time when 404 was cached
	cacheMu          sync.RWMutex
	cacheTTL         time.Duration
	negativeCacheTTL time.Duration // TTL for negative (404) cache entries
}

// CachedSchema represents a cached schema with metadata
type CachedSchema struct {
	ID       uint32    `json:"id"`
	Schema   string    `json:"schema"`
	Subject  string    `json:"subject"`
	Version  int       `json:"version"`
	Format   Format    `json:"-"` // Derived from schema content
	CachedAt time.Time `json:"-"`
}

// CachedSubject represents cached subject information
type CachedSubject struct {
	Subject  string    `json:"subject"`
	LatestID uint32    `json:"id"`
	Version  int       `json:"version"`
	Schema   string    `json:"schema"`
	CachedAt time.Time `json:"-"`
}

// RegistryConfig holds configuration for the Schema Registry client
type RegistryConfig struct {
	URL        string
	Username   string // Optional basic auth
	Password   string // Optional basic auth
	Timeout    time.Duration
	CacheTTL   time.Duration
	MaxRetries int
}

// NewRegistryClient creates a new Schema Registry client
func NewRegistryClient(config RegistryConfig) *RegistryClient {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute
	}

	httpClient := &http.Client{
		Timeout: config.Timeout,
	}

	return &RegistryClient{
		baseURL:          config.URL,
		httpClient:       httpClient,
		schemaCache:      make(map[uint32]*CachedSchema),
		subjectCache:     make(map[string]*CachedSubject),
		negativeCache:    make(map[string]time.Time),
		cacheTTL:         config.CacheTTL,
		negativeCacheTTL: 2 * time.Minute, // Cache 404s for 2 minutes
	}
}

// GetSchemaByID retrieves a schema by its ID
func (rc *RegistryClient) GetSchemaByID(schemaID uint32) (*CachedSchema, error) {
	// Check cache first
	rc.cacheMu.RLock()
	if cached, exists := rc.schemaCache[schemaID]; exists {
		if time.Since(cached.CachedAt) < rc.cacheTTL {
			rc.cacheMu.RUnlock()
			return cached, nil
		}
	}
	rc.cacheMu.RUnlock()

	// Fetch from registry
	url := fmt.Sprintf("%s/schemas/ids/%d", rc.baseURL, schemaID)
	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema %d: %w", schemaID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error %d: %s", resp.StatusCode, string(body))
	}

	var schemaResp struct {
		Schema  string `json:"schema"`
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}

	// Determine format from schema content
	format := rc.detectSchemaFormat(schemaResp.Schema)

	cached := &CachedSchema{
		ID:       schemaID,
		Schema:   schemaResp.Schema,
		Subject:  schemaResp.Subject,
		Version:  schemaResp.Version,
		Format:   format,
		CachedAt: time.Now(),
	}

	// Update cache
	rc.cacheMu.Lock()
	rc.schemaCache[schemaID] = cached
	rc.cacheMu.Unlock()

	return cached, nil
}

// GetLatestSchema retrieves the latest schema for a subject
func (rc *RegistryClient) GetLatestSchema(subject string) (*CachedSubject, error) {
	// Check positive cache first
	rc.cacheMu.RLock()
	if cached, exists := rc.subjectCache[subject]; exists {
		if time.Since(cached.CachedAt) < rc.cacheTTL {
			rc.cacheMu.RUnlock()
			return cached, nil
		}
	}

	// Check negative cache (404 cache)
	if cachedAt, exists := rc.negativeCache[subject]; exists {
		if time.Since(cachedAt) < rc.negativeCacheTTL {
			rc.cacheMu.RUnlock()
			return nil, fmt.Errorf("schema registry error 404: subject not found (cached)")
		}
	}
	rc.cacheMu.RUnlock()

	// Fetch from registry
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", rc.baseURL, subject)
	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest schema for %s: %w", subject, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		// Cache 404 responses to avoid repeated lookups
		if resp.StatusCode == http.StatusNotFound {
			rc.cacheMu.Lock()
			rc.negativeCache[subject] = time.Now()
			rc.cacheMu.Unlock()
		}

		return nil, fmt.Errorf("schema registry error %d: %s", resp.StatusCode, string(body))
	}

	var schemaResp struct {
		ID      uint32 `json:"id"`
		Schema  string `json:"schema"`
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}

	cached := &CachedSubject{
		Subject:  subject,
		LatestID: schemaResp.ID,
		Version:  schemaResp.Version,
		Schema:   schemaResp.Schema,
		CachedAt: time.Now(),
	}

	// Update cache and clear negative cache entry
	rc.cacheMu.Lock()
	rc.subjectCache[subject] = cached
	delete(rc.negativeCache, subject) // Clear any cached 404
	rc.cacheMu.Unlock()

	return cached, nil
}

// RegisterSchema registers a new schema for a subject
func (rc *RegistryClient) RegisterSchema(subject, schema string) (uint32, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", rc.baseURL, subject)

	reqBody := map[string]string{
		"schema": schema,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal schema request: %w", err)
	}

	resp, err := rc.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registry error %d: %s", resp.StatusCode, string(body))
	}

	var regResp struct {
		ID uint32 `json:"id"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return 0, fmt.Errorf("failed to decode registration response: %w", err)
	}

	// Invalidate caches for this subject
	rc.cacheMu.Lock()
	delete(rc.subjectCache, subject)
	delete(rc.negativeCache, subject) // Clear any cached 404
	// Note: we don't cache the new schema here since we don't have full metadata
	rc.cacheMu.Unlock()

	return regResp.ID, nil
}

// CheckCompatibility checks if a schema is compatible with the subject
func (rc *RegistryClient) CheckCompatibility(subject, schema string) (bool, error) {
	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", rc.baseURL, subject)

	reqBody := map[string]string{
		"schema": schema,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return false, fmt.Errorf("failed to marshal compatibility request: %w", err)
	}

	resp, err := rc.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return false, fmt.Errorf("failed to check compatibility: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("schema registry error %d: %s", resp.StatusCode, string(body))
	}

	var compatResp struct {
		IsCompatible bool `json:"is_compatible"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&compatResp); err != nil {
		return false, fmt.Errorf("failed to decode compatibility response: %w", err)
	}

	return compatResp.IsCompatible, nil
}

// ListSubjects returns all subjects in the registry
func (rc *RegistryClient) ListSubjects() ([]string, error) {
	url := fmt.Sprintf("%s/subjects", rc.baseURL)
	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to list subjects: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error %d: %s", resp.StatusCode, string(body))
	}

	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		return nil, fmt.Errorf("failed to decode subjects response: %w", err)
	}

	return subjects, nil
}

// ClearCache clears all cached schemas and subjects
func (rc *RegistryClient) ClearCache() {
	rc.cacheMu.Lock()
	defer rc.cacheMu.Unlock()

	rc.schemaCache = make(map[uint32]*CachedSchema)
	rc.subjectCache = make(map[string]*CachedSubject)
	rc.negativeCache = make(map[string]time.Time)
}

// GetCacheStats returns cache statistics
func (rc *RegistryClient) GetCacheStats() (schemaCount, subjectCount, negativeCacheCount int) {
	rc.cacheMu.RLock()
	defer rc.cacheMu.RUnlock()

	return len(rc.schemaCache), len(rc.subjectCache), len(rc.negativeCache)
}

// detectSchemaFormat attempts to determine the schema format from content
func (rc *RegistryClient) detectSchemaFormat(schema string) Format {
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
					// Note: "string" is ambiguous - it could be Avro primitive or JSON Schema
					// We need to check other indicators first
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
		// Default JSON-based schema to Avro only if it doesn't look like JSON Schema
		return FormatAvro
	}

	// Check for Protobuf (typically not JSON)
	// Protobuf schemas in Schema Registry are usually stored as descriptors
	// For now, assume non-JSON schemas are Protobuf
	return FormatProtobuf
}

// HealthCheck verifies the registry is accessible
func (rc *RegistryClient) HealthCheck() error {
	url := fmt.Sprintf("%s/subjects", rc.baseURL)
	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("schema registry health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("schema registry health check failed with status %d", resp.StatusCode)
	}

	return nil
}
