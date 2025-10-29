package schema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewRegistryClient(t *testing.T) {
	config := RegistryConfig{
		URL: "http://localhost:8081",
	}

	client := NewRegistryClient(config)

	if client.baseURL != config.URL {
		t.Errorf("Expected baseURL %s, got %s", config.URL, client.baseURL)
	}

	if client.cacheTTL != 5*time.Minute {
		t.Errorf("Expected default cacheTTL 5m, got %v", client.cacheTTL)
	}

	if client.httpClient.Timeout != 30*time.Second {
		t.Errorf("Expected default timeout 30s, got %v", client.httpClient.Timeout)
	}
}

func TestRegistryClient_GetSchemaByID(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/schemas/ids/1" {
			response := map[string]interface{}{
				"schema":  `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`,
				"subject": "user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)
		} else if r.URL.Path == "/schemas/ids/999" {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error_code":40403,"message":"Schema not found"}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := RegistryConfig{
		URL:      server.URL,
		CacheTTL: 1 * time.Minute,
	}
	client := NewRegistryClient(config)

	t.Run("successful fetch", func(t *testing.T) {
		schema, err := client.GetSchemaByID(1)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if schema.ID != 1 {
			t.Errorf("Expected schema ID 1, got %d", schema.ID)
		}

		if schema.Subject != "user-value" {
			t.Errorf("Expected subject 'user-value', got %s", schema.Subject)
		}

		if schema.Format != FormatAvro {
			t.Errorf("Expected Avro format, got %v", schema.Format)
		}
	})

	t.Run("schema not found", func(t *testing.T) {
		_, err := client.GetSchemaByID(999)
		if err == nil {
			t.Fatal("Expected error for non-existent schema")
		}
	})

	t.Run("cache hit", func(t *testing.T) {
		// First call should cache the result
		schema1, err := client.GetSchemaByID(1)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Second call should hit cache (same timestamp)
		schema2, err := client.GetSchemaByID(1)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if schema1.CachedAt != schema2.CachedAt {
			t.Error("Expected cache hit with same timestamp")
		}
	})
}

func TestRegistryClient_GetLatestSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/subjects/user-value/versions/latest" {
			response := map[string]interface{}{
				"id":      uint32(1),
				"schema":  `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`,
				"subject": "user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := RegistryConfig{URL: server.URL}
	client := NewRegistryClient(config)

	schema, err := client.GetLatestSchema("user-value")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if schema.LatestID != 1 {
		t.Errorf("Expected schema ID 1, got %d", schema.LatestID)
	}

	if schema.Subject != "user-value" {
		t.Errorf("Expected subject 'user-value', got %s", schema.Subject)
	}
}

func TestRegistryClient_RegisterSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/subjects/test-value/versions" {
			response := map[string]interface{}{
				"id": uint32(123),
			}
			json.NewEncoder(w).Encode(response)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := RegistryConfig{URL: server.URL}
	client := NewRegistryClient(config)

	schemaStr := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
	id, err := client.RegisterSchema("test-value", schemaStr)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if id != 123 {
		t.Errorf("Expected schema ID 123, got %d", id)
	}
}

func TestRegistryClient_CheckCompatibility(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/compatibility/subjects/test-value/versions/latest" {
			response := map[string]interface{}{
				"is_compatible": true,
			}
			json.NewEncoder(w).Encode(response)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := RegistryConfig{URL: server.URL}
	client := NewRegistryClient(config)

	schemaStr := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
	compatible, err := client.CheckCompatibility("test-value", schemaStr)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !compatible {
		t.Error("Expected schema to be compatible")
	}
}

func TestRegistryClient_ListSubjects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/subjects" {
			subjects := []string{"user-value", "order-value", "product-key"}
			json.NewEncoder(w).Encode(subjects)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := RegistryConfig{URL: server.URL}
	client := NewRegistryClient(config)

	subjects, err := client.ListSubjects()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expectedSubjects := []string{"user-value", "order-value", "product-key"}
	if len(subjects) != len(expectedSubjects) {
		t.Errorf("Expected %d subjects, got %d", len(expectedSubjects), len(subjects))
	}

	for i, expected := range expectedSubjects {
		if subjects[i] != expected {
			t.Errorf("Expected subject %s, got %s", expected, subjects[i])
		}
	}
}

func TestRegistryClient_DetectSchemaFormat(t *testing.T) {
	config := RegistryConfig{URL: "http://localhost:8081"}
	client := NewRegistryClient(config)

	tests := []struct {
		name     string
		schema   string
		expected Format
	}{
		{
			name:     "Avro record schema",
			schema:   `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`,
			expected: FormatAvro,
		},
		{
			name:     "Avro enum schema",
			schema:   `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`,
			expected: FormatAvro,
		},
		{
			name:     "JSON Schema",
			schema:   `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}`,
			expected: FormatJSONSchema,
		},
		{
			name:     "Protobuf (non-JSON)",
			schema:   "syntax = \"proto3\"; message User { int32 id = 1; }",
			expected: FormatProtobuf,
		},
		{
			name:     "Simple Avro primitive",
			schema:   `{"type":"string"}`,
			expected: FormatAvro,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format := client.detectSchemaFormat(tt.schema)
			if format != tt.expected {
				t.Errorf("Expected format %v, got %v", tt.expected, format)
			}
		})
	}
}

func TestRegistryClient_CacheManagement(t *testing.T) {
	config := RegistryConfig{
		URL:      "http://localhost:8081",
		CacheTTL: 100 * time.Millisecond, // Short TTL for testing
	}
	client := NewRegistryClient(config)

	// Add some cache entries manually
	client.schemaCache[1] = &CachedSchema{
		ID:       1,
		Schema:   "test",
		CachedAt: time.Now(),
	}
	client.subjectCache["test"] = &CachedSubject{
		Subject:  "test",
		CachedAt: time.Now(),
	}

	// Check cache stats
	schemaCount, subjectCount, _ := client.GetCacheStats()
	if schemaCount != 1 || subjectCount != 1 {
		t.Errorf("Expected 1 schema and 1 subject in cache, got %d and %d", schemaCount, subjectCount)
	}

	// Clear cache
	client.ClearCache()
	schemaCount, subjectCount, _ = client.GetCacheStats()
	if schemaCount != 0 || subjectCount != 0 {
		t.Errorf("Expected empty cache after clear, got %d schemas and %d subjects", schemaCount, subjectCount)
	}
}

func TestRegistryClient_HealthCheck(t *testing.T) {
	t.Run("healthy registry", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/subjects" {
				json.NewEncoder(w).Encode([]string{})
			}
		}))
		defer server.Close()

		config := RegistryConfig{URL: server.URL}
		client := NewRegistryClient(config)

		err := client.HealthCheck()
		if err != nil {
			t.Errorf("Expected healthy registry, got error: %v", err)
		}
	})

	t.Run("unhealthy registry", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		config := RegistryConfig{URL: server.URL}
		client := NewRegistryClient(config)

		err := client.HealthCheck()
		if err == nil {
			t.Error("Expected error for unhealthy registry")
		}
	})
}

// Benchmark tests
func BenchmarkRegistryClient_GetSchemaByID(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"schema":  `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`,
			"subject": "user-value",
			"version": 1,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	config := RegistryConfig{URL: server.URL}
	client := NewRegistryClient(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = client.GetSchemaByID(1)
	}
}

func BenchmarkRegistryClient_DetectSchemaFormat(b *testing.B) {
	config := RegistryConfig{URL: "http://localhost:8081"}
	client := NewRegistryClient(config)

	avroSchema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = client.detectSchemaFormat(avroSchema)
	}
}
