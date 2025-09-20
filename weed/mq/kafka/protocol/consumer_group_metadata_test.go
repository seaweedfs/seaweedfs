package protocol

import (
	"net"
	"reflect"
	"testing"
)

func TestExtractClientHost(t *testing.T) {
	tests := []struct {
		name     string
		connCtx  *ConnectionContext
		expected string
	}{
		{
			name:     "Nil connection context",
			connCtx:  nil,
			expected: "unknown",
		},
		{
			name: "TCP address",
			connCtx: &ConnectionContext{
				RemoteAddr: &net.TCPAddr{
					IP:   net.ParseIP("192.168.1.100"),
					Port: 54321,
				},
			},
			expected: "192.168.1.100",
		},
		{
			name: "TCP address with IPv6",
			connCtx: &ConnectionContext{
				RemoteAddr: &net.TCPAddr{
					IP:   net.ParseIP("::1"),
					Port: 54321,
				},
			},
			expected: "::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractClientHost(tt.connCtx)
			if result != tt.expected {
				t.Errorf("ExtractClientHost() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseConsumerProtocolMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata []byte
		strategy string
		want     *ConsumerProtocolMetadata
		wantErr  bool
	}{
		{
			name:     "Empty metadata",
			metadata: []byte{},
			strategy: "range",
			want: &ConsumerProtocolMetadata{
				Version:            0,
				Topics:             []string{},
				UserData:           []byte{},
				AssignmentStrategy: "range",
			},
			wantErr: false,
		},
		{
			name: "Valid metadata with topics",
			metadata: func() []byte {
				data := make([]byte, 0)
				// Version (2 bytes)
				data = append(data, 0, 1)
				// Topics count (4 bytes) - 2 topics
				data = append(data, 0, 0, 0, 2)
				// Topic 1: "topic-a"
				data = append(data, 0, 7) // length
				data = append(data, []byte("topic-a")...)
				// Topic 2: "topic-b"
				data = append(data, 0, 7) // length
				data = append(data, []byte("topic-b")...)
				// UserData length (4 bytes) - 5 bytes
				data = append(data, 0, 0, 0, 5)
				// UserData content
				data = append(data, []byte("hello")...)
				return data
			}(),
			strategy: "roundrobin",
			want: &ConsumerProtocolMetadata{
				Version:            1,
				Topics:             []string{"topic-a", "topic-b"},
				UserData:           []byte("hello"),
				AssignmentStrategy: "roundrobin",
			},
			wantErr: false,
		},
		{
			name:     "Metadata too short for version (handled gracefully)",
			metadata: []byte{0}, // Only 1 byte
			strategy: "range",
			want: &ConsumerProtocolMetadata{
				Version:            0,
				Topics:             []string{},
				UserData:           []byte{},
				AssignmentStrategy: "range",
			},
			wantErr: false, // Should handle gracefully, not error
		},
		{
			name: "Unreasonable topics count",
			metadata: func() []byte {
				data := make([]byte, 0)
				data = append(data, 0, 1)                   // version
				data = append(data, 0xFF, 0xFF, 0xFF, 0xFF) // huge topics count
				return data
			}(),
			strategy: "range",
			want:     nil,
			wantErr:  true,
		},
		{
			name: "Topic name too long",
			metadata: func() []byte {
				data := make([]byte, 0)
				data = append(data, 0, 1)       // version
				data = append(data, 0, 0, 0, 1) // 1 topic
				data = append(data, 0xFF, 0xFF) // huge topic name length
				return data
			}(),
			strategy: "sticky",
			want:     nil,
			wantErr:  true,
		},
		{
			name: "Valid metadata with empty topic name (should skip)",
			metadata: func() []byte {
				data := make([]byte, 0)
				data = append(data, 0, 1)       // version
				data = append(data, 0, 0, 0, 2) // 2 topics
				// Topic 1: empty name
				data = append(data, 0, 0) // length 0
				// Topic 2: "valid-topic"
				data = append(data, 0, 11) // length
				data = append(data, []byte("valid-topic")...)
				// UserData length (4 bytes) - 0 bytes
				data = append(data, 0, 0, 0, 0)
				return data
			}(),
			strategy: "range",
			want: &ConsumerProtocolMetadata{
				Version:            1,
				Topics:             []string{"valid-topic"},
				UserData:           []byte{},
				AssignmentStrategy: "range",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseConsumerProtocolMetadata(tt.metadata, tt.strategy)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConsumerProtocolMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConsumerProtocolMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerateConsumerProtocolMetadata(t *testing.T) {
	tests := []struct {
		name     string
		topics   []string
		userData []byte
	}{
		{
			name:     "No topics, no user data",
			topics:   []string{},
			userData: []byte{},
		},
		{
			name:     "Single topic, no user data",
			topics:   []string{"test-topic"},
			userData: []byte{},
		},
		{
			name:     "Multiple topics with user data",
			topics:   []string{"topic-1", "topic-2", "topic-3"},
			userData: []byte("user-data-content"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Generate metadata
			generated := GenerateConsumerProtocolMetadata(tt.topics, tt.userData)

			// Parse it back
			parsed, err := ParseConsumerProtocolMetadata(generated, "test")
			if err != nil {
				t.Fatalf("Failed to parse generated metadata: %v", err)
			}

			// Verify topics match
			if !reflect.DeepEqual(parsed.Topics, tt.topics) {
				t.Errorf("Generated topics = %v, want %v", parsed.Topics, tt.topics)
			}

			// Verify user data matches
			if !reflect.DeepEqual(parsed.UserData, tt.userData) {
				t.Errorf("Generated user data = %v, want %v", parsed.UserData, tt.userData)
			}

			// Verify version is 1
			if parsed.Version != 1 {
				t.Errorf("Generated version = %v, want 1", parsed.Version)
			}
		})
	}
}

func TestValidateAssignmentStrategy(t *testing.T) {
	tests := []struct {
		strategy string
		valid    bool
	}{
		{"range", true},
		{"roundrobin", true},
		{"sticky", true},
		{"cooperative-sticky", false}, // Not implemented yet
		{"unknown", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			result := ValidateAssignmentStrategy(tt.strategy)
			if result != tt.valid {
				t.Errorf("ValidateAssignmentStrategy(%s) = %v, want %v", tt.strategy, result, tt.valid)
			}
		})
	}
}

func TestExtractTopicsFromMetadata(t *testing.T) {
	// Create test metadata for range protocol
	rangeMetadata := GenerateConsumerProtocolMetadata([]string{"topic-a", "topic-b"}, []byte{})
	roundrobinMetadata := GenerateConsumerProtocolMetadata([]string{"topic-x", "topic-y"}, []byte{})
	invalidMetadata := []byte{0xFF, 0xFF} // Invalid metadata

	tests := []struct {
		name           string
		protocols      []GroupProtocol
		fallbackTopics []string
		expectedTopics []string
	}{
		{
			name: "Extract from range protocol",
			protocols: []GroupProtocol{
				{Name: "range", Metadata: rangeMetadata},
				{Name: "roundrobin", Metadata: roundrobinMetadata},
			},
			fallbackTopics: []string{"fallback"},
			expectedTopics: []string{"topic-a", "topic-b"},
		},
		{
			name: "Invalid metadata, use fallback",
			protocols: []GroupProtocol{
				{Name: "range", Metadata: invalidMetadata},
			},
			fallbackTopics: []string{"fallback-topic"},
			expectedTopics: []string{"fallback-topic"},
		},
		{
			name:           "No protocols, use fallback",
			protocols:      []GroupProtocol{},
			fallbackTopics: []string{"fallback-topic"},
			expectedTopics: []string{"fallback-topic"},
		},
		{
			name:           "No protocols, no fallback, use default",
			protocols:      []GroupProtocol{},
			fallbackTopics: []string{},
			expectedTopics: []string{"test-topic"},
		},
		{
			name: "Unsupported protocol, use fallback",
			protocols: []GroupProtocol{
				{Name: "unsupported", Metadata: rangeMetadata},
			},
			fallbackTopics: []string{"fallback-topic"},
			expectedTopics: []string{"fallback-topic"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractTopicsFromMetadata(tt.protocols, tt.fallbackTopics)
			if !reflect.DeepEqual(result, tt.expectedTopics) {
				t.Errorf("ExtractTopicsFromMetadata() = %v, want %v", result, tt.expectedTopics)
			}
		})
	}
}

func TestSelectBestProtocol(t *testing.T) {
	tests := []struct {
		name            string
		clientProtocols []GroupProtocol
		groupProtocols  []string
		expected        string
	}{
		{
			name: "Prefer sticky over roundrobin",
			clientProtocols: []GroupProtocol{
				{Name: "range", Metadata: []byte{}},
				{Name: "roundrobin", Metadata: []byte{}},
				{Name: "sticky", Metadata: []byte{}},
			},
			groupProtocols: []string{"range", "roundrobin", "sticky"},
			expected:       "sticky",
		},
		{
			name: "Prefer roundrobin over range",
			clientProtocols: []GroupProtocol{
				{Name: "range", Metadata: []byte{}},
				{Name: "roundrobin", Metadata: []byte{}},
			},
			groupProtocols: []string{"range", "roundrobin"},
			expected:       "roundrobin",
		},
		{
			name: "Only range available",
			clientProtocols: []GroupProtocol{
				{Name: "range", Metadata: []byte{}},
			},
			groupProtocols: []string{"range"},
			expected:       "range",
		},
		{
			name: "Client supports sticky but group doesn't",
			clientProtocols: []GroupProtocol{
				{Name: "sticky", Metadata: []byte{}},
				{Name: "range", Metadata: []byte{}},
			},
			groupProtocols: []string{"range", "roundrobin"},
			expected:       "range",
		},
		{
			name: "No group protocols specified (new group)",
			clientProtocols: []GroupProtocol{
				{Name: "sticky", Metadata: []byte{}},
				{Name: "roundrobin", Metadata: []byte{}},
			},
			groupProtocols: []string{}, // Empty = new group
			expected:       "sticky",
		},
		{
			name: "No supported protocols, fallback to range",
			clientProtocols: []GroupProtocol{
				{Name: "unsupported", Metadata: []byte{}},
			},
			groupProtocols: []string{"range"},
			expected:       "range", // Last resort fallback
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectBestProtocol(tt.clientProtocols, tt.groupProtocols)
			if result != tt.expected {
				t.Errorf("SelectBestProtocol() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSanitizeConsumerGroupID(t *testing.T) {
	tests := []struct {
		name    string
		groupID string
		want    string
		wantErr bool
	}{
		{
			name:    "Valid group ID",
			groupID: "test-group",
			want:    "test-group",
			wantErr: false,
		},
		{
			name:    "Group ID with spaces (trimmed)",
			groupID: "  spaced-group  ",
			want:    "spaced-group",
			wantErr: false,
		},
		{
			name:    "Empty group ID",
			groupID: "",
			want:    "",
			wantErr: true,
		},
		{
			name:    "Group ID too long",
			groupID: string(make([]byte, 256)), // 256 characters
			want:    "",
			wantErr: true,
		},
		{
			name:    "Group ID with control characters",
			groupID: "test\x00group",
			want:    "",
			wantErr: true,
		},
		{
			name:    "Group ID with tab character",
			groupID: "test\tgroup",
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizeConsumerGroupID(tt.groupID)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizeConsumerGroupID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SanitizeConsumerGroupID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnalyzeProtocolMetadata(t *testing.T) {
	// Create valid metadata
	validMetadata := GenerateConsumerProtocolMetadata([]string{"topic-1", "topic-2"}, []byte("userdata"))

	// Create invalid metadata
	invalidMetadata := []byte{0xFF}

	protocols := []GroupProtocol{
		{Name: "range", Metadata: validMetadata},
		{Name: "roundrobin", Metadata: invalidMetadata},
		{Name: "sticky", Metadata: []byte{}}, // Empty but should not error
	}

	result := AnalyzeProtocolMetadata(protocols)

	if len(result) != 3 {
		t.Fatalf("Expected 3 protocol analyses, got %d", len(result))
	}

	// Check range protocol (should parse successfully)
	rangeInfo := result[0]
	if rangeInfo.Strategy != "range" {
		t.Errorf("Expected strategy 'range', got '%s'", rangeInfo.Strategy)
	}
	if !rangeInfo.ParsedOK {
		t.Errorf("Expected range protocol to parse successfully")
	}
	if rangeInfo.TopicCount != 2 {
		t.Errorf("Expected 2 topics, got %d", rangeInfo.TopicCount)
	}

	// Check roundrobin protocol (with invalid metadata, handled gracefully)
	roundrobinInfo := result[1]
	if roundrobinInfo.Strategy != "roundrobin" {
		t.Errorf("Expected strategy 'roundrobin', got '%s'", roundrobinInfo.Strategy)
	}
	// Note: We now handle invalid metadata gracefully, so it should parse successfully with empty topics
	if !roundrobinInfo.ParsedOK {
		t.Errorf("Expected roundrobin protocol to be handled gracefully")
	}
	if roundrobinInfo.TopicCount != 0 {
		t.Errorf("Expected 0 topics for invalid metadata, got %d", roundrobinInfo.TopicCount)
	}

	// Check sticky protocol (empty metadata should not error but return empty topics)
	stickyInfo := result[2]
	if stickyInfo.Strategy != "sticky" {
		t.Errorf("Expected strategy 'sticky', got '%s'", stickyInfo.Strategy)
	}
	if !stickyInfo.ParsedOK {
		t.Errorf("Expected empty metadata to parse successfully")
	}
	if stickyInfo.TopicCount != 0 {
		t.Errorf("Expected 0 topics for empty metadata, got %d", stickyInfo.TopicCount)
	}
}

// Benchmark tests for performance validation
func BenchmarkParseConsumerProtocolMetadata(b *testing.B) {
	// Create realistic metadata with multiple topics
	topics := []string{"topic-1", "topic-2", "topic-3", "topic-4", "topic-5"}
	userData := []byte("some-user-data-content")
	metadata := GenerateConsumerProtocolMetadata(topics, userData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseConsumerProtocolMetadata(metadata, "range")
	}
}

func BenchmarkExtractClientHost(b *testing.B) {
	connCtx := &ConnectionContext{
		RemoteAddr: &net.TCPAddr{
			IP:   net.ParseIP("192.168.1.100"),
			Port: 54321,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ExtractClientHost(connCtx)
	}
}

func BenchmarkSelectBestProtocol(b *testing.B) {
	protocols := []GroupProtocol{
		{Name: "range", Metadata: []byte{}},
		{Name: "roundrobin", Metadata: []byte{}},
		{Name: "sticky", Metadata: []byte{}},
	}
	groupProtocols := []string{"range", "roundrobin", "sticky"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SelectBestProtocol(protocols, groupProtocols)
	}
}
