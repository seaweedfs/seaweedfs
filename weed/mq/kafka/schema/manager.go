package schema

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// Manager coordinates schema operations for the Kafka Gateway
type Manager struct {
	registryClient *RegistryClient
	
	// Decoder cache
	avroDecoders map[uint32]*AvroDecoder // schema ID -> decoder
	decoderMu    sync.RWMutex
	
	// Configuration
	config ManagerConfig
}

// ManagerConfig holds configuration for the schema manager
type ManagerConfig struct {
	RegistryURL      string
	RegistryUsername string
	RegistryPassword string
	CacheTTL         string
	ValidationMode   ValidationMode
	EnableMirroring  bool
	MirrorPath       string // Path in SeaweedFS Filer to mirror schemas
}

// ValidationMode defines how strict schema validation should be
type ValidationMode int

const (
	ValidationPermissive ValidationMode = iota // Allow unknown fields, best-effort decoding
	ValidationStrict                           // Reject messages that don't match schema exactly
)

// DecodedMessage represents a decoded Kafka message with schema information
type DecodedMessage struct {
	// Original envelope information
	Envelope *ConfluentEnvelope
	
	// Schema information
	SchemaID     uint32
	SchemaFormat Format
	Subject      string
	Version      int
	
	// Decoded data
	RecordValue *schema_pb.RecordValue
	RecordType  *schema_pb.RecordType
	
	// Metadata for storage
	Metadata map[string]string
}

// NewManager creates a new schema manager
func NewManager(config ManagerConfig) (*Manager, error) {
	registryConfig := RegistryConfig{
		URL:      config.RegistryURL,
		Username: config.RegistryUsername,
		Password: config.RegistryPassword,
	}
	
	registryClient := NewRegistryClient(registryConfig)
	
	return &Manager{
		registryClient: registryClient,
		avroDecoders:   make(map[uint32]*AvroDecoder),
		config:         config,
	}, nil
}

// NewManagerWithHealthCheck creates a new schema manager and validates connectivity
func NewManagerWithHealthCheck(config ManagerConfig) (*Manager, error) {
	manager, err := NewManager(config)
	if err != nil {
		return nil, err
	}
	
	// Test connectivity
	if err := manager.registryClient.HealthCheck(); err != nil {
		return nil, fmt.Errorf("schema registry health check failed: %w", err)
	}
	
	return manager, nil
}

// DecodeMessage decodes a Kafka message if it contains schema information
func (m *Manager) DecodeMessage(messageBytes []byte) (*DecodedMessage, error) {
	// Step 1: Check if message is schematized
	envelope, isSchematized := ParseConfluentEnvelope(messageBytes)
	if !isSchematized {
		return nil, fmt.Errorf("message is not schematized")
	}
	
	// Step 2: Validate envelope
	if err := envelope.Validate(); err != nil {
		return nil, fmt.Errorf("invalid envelope: %w", err)
	}
	
	// Step 3: Get schema from registry
	cachedSchema, err := m.registryClient.GetSchemaByID(envelope.SchemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema %d: %w", envelope.SchemaID, err)
	}
	
	// Step 4: Decode based on format
	var recordValue *schema_pb.RecordValue
	var recordType *schema_pb.RecordType
	
	switch cachedSchema.Format {
	case FormatAvro:
		recordValue, recordType, err = m.decodeAvroMessage(envelope, cachedSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Avro message: %w", err)
		}
	case FormatProtobuf:
		return nil, fmt.Errorf("Protobuf decoding not yet implemented (Phase 5)")
	case FormatJSONSchema:
		return nil, fmt.Errorf("JSON Schema decoding not yet implemented (Phase 6)")
	default:
		return nil, fmt.Errorf("unsupported schema format: %v", cachedSchema.Format)
	}
	
	// Step 5: Create decoded message
	decodedMsg := &DecodedMessage{
		Envelope:     envelope,
		SchemaID:     envelope.SchemaID,
		SchemaFormat: cachedSchema.Format,
		Subject:      cachedSchema.Subject,
		Version:      cachedSchema.Version,
		RecordValue:  recordValue,
		RecordType:   recordType,
		Metadata:     m.createMetadata(envelope, cachedSchema),
	}
	
	return decodedMsg, nil
}

// decodeAvroMessage decodes an Avro message using cached or new decoder
func (m *Manager) decodeAvroMessage(envelope *ConfluentEnvelope, cachedSchema *CachedSchema) (*schema_pb.RecordValue, *schema_pb.RecordType, error) {
	// Get or create Avro decoder
	decoder, err := m.getAvroDecoder(envelope.SchemaID, cachedSchema.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get Avro decoder: %w", err)
	}
	
	// Decode to RecordValue
	recordValue, err := decoder.DecodeToRecordValue(envelope.Payload)
	if err != nil {
		if m.config.ValidationMode == ValidationStrict {
			return nil, nil, fmt.Errorf("strict validation failed: %w", err)
		}
		// In permissive mode, try to decode as much as possible
		// For now, return the error - we could implement partial decoding later
		return nil, nil, fmt.Errorf("permissive decoding failed: %w", err)
	}
	
	// Infer or get RecordType
	recordType, err := decoder.InferRecordType()
	if err != nil {
		// Fall back to inferring from the decoded map
		if decodedMap, decodeErr := decoder.Decode(envelope.Payload); decodeErr == nil {
			recordType = InferRecordTypeFromMap(decodedMap)
		} else {
			return nil, nil, fmt.Errorf("failed to infer record type: %w", err)
		}
	}
	
	return recordValue, recordType, nil
}

// getAvroDecoder gets or creates an Avro decoder for the given schema
func (m *Manager) getAvroDecoder(schemaID uint32, schemaStr string) (*AvroDecoder, error) {
	// Check cache first
	m.decoderMu.RLock()
	if decoder, exists := m.avroDecoders[schemaID]; exists {
		m.decoderMu.RUnlock()
		return decoder, nil
	}
	m.decoderMu.RUnlock()
	
	// Create new decoder
	decoder, err := NewAvroDecoder(schemaStr)
	if err != nil {
		return nil, err
	}
	
	// Cache the decoder
	m.decoderMu.Lock()
	m.avroDecoders[schemaID] = decoder
	m.decoderMu.Unlock()
	
	return decoder, nil
}

// createMetadata creates metadata for storage in SeaweedMQ
func (m *Manager) createMetadata(envelope *ConfluentEnvelope, cachedSchema *CachedSchema) map[string]string {
	metadata := envelope.Metadata()
	
	// Add schema registry information
	metadata["schema_subject"] = cachedSchema.Subject
	metadata["schema_version"] = fmt.Sprintf("%d", cachedSchema.Version)
	metadata["registry_url"] = m.registryClient.baseURL
	
	// Add decoding information
	metadata["decoded_at"] = fmt.Sprintf("%d", cachedSchema.CachedAt.Unix())
	metadata["validation_mode"] = fmt.Sprintf("%d", m.config.ValidationMode)
	
	return metadata
}

// IsSchematized checks if a message contains schema information
func (m *Manager) IsSchematized(messageBytes []byte) bool {
	return IsSchematized(messageBytes)
}

// GetSchemaInfo extracts basic schema information without full decoding
func (m *Manager) GetSchemaInfo(messageBytes []byte) (uint32, Format, error) {
	envelope, ok := ParseConfluentEnvelope(messageBytes)
	if !ok {
		return 0, FormatUnknown, fmt.Errorf("not a schematized message")
	}
	
	// Get basic schema info from cache or registry
	cachedSchema, err := m.registryClient.GetSchemaByID(envelope.SchemaID)
	if err != nil {
		return 0, FormatUnknown, fmt.Errorf("failed to get schema info: %w", err)
	}
	
	return envelope.SchemaID, cachedSchema.Format, nil
}

// RegisterSchema registers a new schema with the registry
func (m *Manager) RegisterSchema(subject, schema string) (uint32, error) {
	return m.registryClient.RegisterSchema(subject, schema)
}

// CheckCompatibility checks if a schema is compatible with existing versions
func (m *Manager) CheckCompatibility(subject, schema string) (bool, error) {
	return m.registryClient.CheckCompatibility(subject, schema)
}

// ListSubjects returns all subjects in the registry
func (m *Manager) ListSubjects() ([]string, error) {
	return m.registryClient.ListSubjects()
}

// ClearCache clears all cached decoders and registry data
func (m *Manager) ClearCache() {
	m.decoderMu.Lock()
	m.avroDecoders = make(map[uint32]*AvroDecoder)
	m.decoderMu.Unlock()
	
	m.registryClient.ClearCache()
}

// GetCacheStats returns cache statistics
func (m *Manager) GetCacheStats() (decoders, schemas, subjects int) {
	m.decoderMu.RLock()
	decoders = len(m.avroDecoders)
	m.decoderMu.RUnlock()
	
	schemas, subjects = m.registryClient.GetCacheStats()
	return
}

// EncodeMessage encodes a RecordValue back to Confluent format (for Fetch path)
func (m *Manager) EncodeMessage(recordValue *schema_pb.RecordValue, schemaID uint32, format Format) ([]byte, error) {
	switch format {
	case FormatAvro:
		return m.encodeAvroMessage(recordValue, schemaID)
	case FormatProtobuf:
		return nil, fmt.Errorf("Protobuf encoding not yet implemented (Phase 7)")
	case FormatJSONSchema:
		return nil, fmt.Errorf("JSON Schema encoding not yet implemented (Phase 7)")
	default:
		return nil, fmt.Errorf("unsupported format for encoding: %v", format)
	}
}

// encodeAvroMessage encodes a RecordValue back to Avro binary format
func (m *Manager) encodeAvroMessage(recordValue *schema_pb.RecordValue, schemaID uint32) ([]byte, error) {
	// Get schema from registry
	cachedSchema, err := m.registryClient.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for encoding: %w", err)
	}
	
	// Get decoder (which contains the codec)
	decoder, err := m.getAvroDecoder(schemaID, cachedSchema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get decoder for encoding: %w", err)
	}
	
	// Convert RecordValue back to Go map
	goMap := recordValueToMap(recordValue)
	
	// Encode using Avro codec
	binary, err := decoder.codec.BinaryFromNative(nil, goMap)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to Avro binary: %w", err)
	}
	
	// Create Confluent envelope
	envelope := CreateConfluentEnvelope(FormatAvro, schemaID, nil, binary)
	
	return envelope, nil
}

// recordValueToMap converts a RecordValue back to a Go map for encoding
func recordValueToMap(recordValue *schema_pb.RecordValue) map[string]interface{} {
	result := make(map[string]interface{})
	
	for key, value := range recordValue.Fields {
		result[key] = schemaValueToGoValue(value)
	}
	
	return result
}

// schemaValueToGoValue converts a schema Value back to a Go value
func schemaValueToGoValue(value *schema_pb.Value) interface{} {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_BoolValue:
		return v.BoolValue
	case *schema_pb.Value_Int32Value:
		return v.Int32Value
	case *schema_pb.Value_Int64Value:
		return v.Int64Value
	case *schema_pb.Value_FloatValue:
		return v.FloatValue
	case *schema_pb.Value_DoubleValue:
		return v.DoubleValue
	case *schema_pb.Value_StringValue:
		return v.StringValue
	case *schema_pb.Value_BytesValue:
		return v.BytesValue
	case *schema_pb.Value_ListValue:
		result := make([]interface{}, len(v.ListValue.Values))
		for i, item := range v.ListValue.Values {
			result[i] = schemaValueToGoValue(item)
		}
		return result
	case *schema_pb.Value_RecordValue:
		return recordValueToMap(v.RecordValue)
	case *schema_pb.Value_TimestampValue:
		// Convert back to time if needed, or return as int64
		return v.TimestampValue.TimestampMicros
	default:
		// Default to string representation
		return fmt.Sprintf("%v", value)
	}
}
