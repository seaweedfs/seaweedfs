package schema

import (
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// Manager coordinates schema operations for the Kafka Gateway
type Manager struct {
	registryClient *RegistryClient

	// Decoder cache
	avroDecoders       map[uint32]*AvroDecoder       // schema ID -> decoder
	protobufDecoders   map[uint32]*ProtobufDecoder   // schema ID -> decoder
	jsonSchemaDecoders map[uint32]*JSONSchemaDecoder // schema ID -> decoder
	decoderMu          sync.RWMutex

	// Schema evolution checker
	evolutionChecker *SchemaEvolutionChecker

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
		registryClient:     registryClient,
		avroDecoders:       make(map[uint32]*AvroDecoder),
		protobufDecoders:   make(map[uint32]*ProtobufDecoder),
		jsonSchemaDecoders: make(map[uint32]*JSONSchemaDecoder),
		evolutionChecker:   NewSchemaEvolutionChecker(),
		config:             config,
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
		recordValue, recordType, err = m.decodeProtobufMessage(envelope, cachedSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Protobuf message: %w", err)
		}
	case FormatJSONSchema:
		recordValue, recordType, err = m.decodeJSONSchemaMessage(envelope, cachedSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to decode JSON Schema message: %w", err)
		}
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

// decodeProtobufMessage decodes a Protobuf message using cached or new decoder
func (m *Manager) decodeProtobufMessage(envelope *ConfluentEnvelope, cachedSchema *CachedSchema) (*schema_pb.RecordValue, *schema_pb.RecordType, error) {
	// Get or create Protobuf decoder
	decoder, err := m.getProtobufDecoder(envelope.SchemaID, cachedSchema.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get Protobuf decoder: %w", err)
	}

	// Decode to RecordValue
	recordValue, err := decoder.DecodeToRecordValue(envelope.Payload)
	if err != nil {
		if m.config.ValidationMode == ValidationStrict {
			return nil, nil, fmt.Errorf("strict validation failed: %w", err)
		}
		// In permissive mode, try to decode as much as possible
		return nil, nil, fmt.Errorf("permissive decoding failed: %w", err)
	}

	// Get RecordType from descriptor
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

// decodeJSONSchemaMessage decodes a JSON Schema message using cached or new decoder
func (m *Manager) decodeJSONSchemaMessage(envelope *ConfluentEnvelope, cachedSchema *CachedSchema) (*schema_pb.RecordValue, *schema_pb.RecordType, error) {
	// Get or create JSON Schema decoder
	decoder, err := m.getJSONSchemaDecoder(envelope.SchemaID, cachedSchema.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get JSON Schema decoder: %w", err)
	}

	// Decode to RecordValue
	recordValue, err := decoder.DecodeToRecordValue(envelope.Payload)
	if err != nil {
		if m.config.ValidationMode == ValidationStrict {
			return nil, nil, fmt.Errorf("strict validation failed: %w", err)
		}
		// In permissive mode, try to decode as much as possible
		return nil, nil, fmt.Errorf("permissive decoding failed: %w", err)
	}

	// Get RecordType from schema
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

// getProtobufDecoder gets or creates a Protobuf decoder for the given schema
func (m *Manager) getProtobufDecoder(schemaID uint32, schemaStr string) (*ProtobufDecoder, error) {
	// Check cache first
	m.decoderMu.RLock()
	if decoder, exists := m.protobufDecoders[schemaID]; exists {
		m.decoderMu.RUnlock()
		return decoder, nil
	}
	m.decoderMu.RUnlock()

	// In Confluent Schema Registry, Protobuf schemas can be stored as:
	// 1. Text .proto format (most common)
	// 2. Binary FileDescriptorSet
	// Try to detect which format we have
	var decoder *ProtobufDecoder
	var err error

	// Check if it looks like text .proto (contains "syntax", "message", etc.)
	if strings.Contains(schemaStr, "syntax") || strings.Contains(schemaStr, "message") {
		// Parse as text .proto
		decoder, err = NewProtobufDecoderFromString(schemaStr)
	} else {
		// Try binary format
		schemaBytes := []byte(schemaStr)
		decoder, err = NewProtobufDecoder(schemaBytes)
	}

	if err != nil {
		return nil, err
	}

	// Cache the decoder
	m.decoderMu.Lock()
	m.protobufDecoders[schemaID] = decoder
	m.decoderMu.Unlock()

	return decoder, nil
}

// getJSONSchemaDecoder gets or creates a JSON Schema decoder for the given schema
func (m *Manager) getJSONSchemaDecoder(schemaID uint32, schemaStr string) (*JSONSchemaDecoder, error) {
	// Check cache first
	m.decoderMu.RLock()
	if decoder, exists := m.jsonSchemaDecoders[schemaID]; exists {
		m.decoderMu.RUnlock()
		return decoder, nil
	}
	m.decoderMu.RUnlock()

	// Create new decoder
	decoder, err := NewJSONSchemaDecoder(schemaStr)
	if err != nil {
		return nil, err
	}

	// Cache the decoder
	m.decoderMu.Lock()
	m.jsonSchemaDecoders[schemaID] = decoder
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
	m.protobufDecoders = make(map[uint32]*ProtobufDecoder)
	m.jsonSchemaDecoders = make(map[uint32]*JSONSchemaDecoder)
	m.decoderMu.Unlock()

	m.registryClient.ClearCache()
}

// GetCacheStats returns cache statistics
func (m *Manager) GetCacheStats() (decoders, schemas, subjects int) {
	m.decoderMu.RLock()
	decoders = len(m.avroDecoders) + len(m.protobufDecoders) + len(m.jsonSchemaDecoders)
	m.decoderMu.RUnlock()

	schemas, subjects, _ = m.registryClient.GetCacheStats()
	return
}

// EncodeMessage encodes a RecordValue back to Confluent format (for Fetch path)
func (m *Manager) EncodeMessage(recordValue *schema_pb.RecordValue, schemaID uint32, format Format) ([]byte, error) {
	switch format {
	case FormatAvro:
		return m.encodeAvroMessage(recordValue, schemaID)
	case FormatProtobuf:
		return m.encodeProtobufMessage(recordValue, schemaID)
	case FormatJSONSchema:
		return m.encodeJSONSchemaMessage(recordValue, schemaID)
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

	// Convert RecordValue back to Go map with Avro union format preservation
	goMap := recordValueToMapWithAvroContext(recordValue, true)

	// Encode using Avro codec
	binary, err := decoder.codec.BinaryFromNative(nil, goMap)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to Avro binary: %w", err)
	}

	// Create Confluent envelope
	envelope := CreateConfluentEnvelope(FormatAvro, schemaID, nil, binary)

	return envelope, nil
}

// encodeProtobufMessage encodes a RecordValue back to Protobuf binary format
func (m *Manager) encodeProtobufMessage(recordValue *schema_pb.RecordValue, schemaID uint32) ([]byte, error) {
	// Get schema from registry
	cachedSchema, err := m.registryClient.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for encoding: %w", err)
	}

	// Get decoder (which contains the descriptor)
	decoder, err := m.getProtobufDecoder(schemaID, cachedSchema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get decoder for encoding: %w", err)
	}

	// Convert RecordValue back to Go map
	goMap := recordValueToMap(recordValue)

	// Create a new message instance and populate it
	msg := decoder.msgType.New()
	if err := m.populateProtobufMessage(msg, goMap, decoder.descriptor); err != nil {
		return nil, fmt.Errorf("failed to populate Protobuf message: %w", err)
	}

	// Encode using Protobuf
	binary, err := proto.Marshal(msg.Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to encode to Protobuf binary: %w", err)
	}

	// Create Confluent envelope (with indexes if needed)
	envelope := CreateConfluentEnvelope(FormatProtobuf, schemaID, nil, binary)

	return envelope, nil
}

// encodeJSONSchemaMessage encodes a RecordValue back to JSON Schema format
func (m *Manager) encodeJSONSchemaMessage(recordValue *schema_pb.RecordValue, schemaID uint32) ([]byte, error) {
	// Get schema from registry
	cachedSchema, err := m.registryClient.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for encoding: %w", err)
	}

	// Get decoder (which contains the schema validator)
	decoder, err := m.getJSONSchemaDecoder(schemaID, cachedSchema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get decoder for encoding: %w", err)
	}

	// Encode using JSON Schema decoder
	jsonData, err := decoder.EncodeFromRecordValue(recordValue)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to JSON: %w", err)
	}

	// Create Confluent envelope
	envelope := CreateConfluentEnvelope(FormatJSONSchema, schemaID, nil, jsonData)

	return envelope, nil
}

// populateProtobufMessage populates a Protobuf message from a Go map
func (m *Manager) populateProtobufMessage(msg protoreflect.Message, data map[string]interface{}, desc protoreflect.MessageDescriptor) error {
	for key, value := range data {
		// Find the field descriptor
		fieldDesc := desc.Fields().ByName(protoreflect.Name(key))
		if fieldDesc == nil {
			// Skip unknown fields in permissive mode
			continue
		}

		// Handle map fields specially
		if fieldDesc.IsMap() {
			if mapData, ok := value.(map[string]interface{}); ok {
				mapValue := msg.Mutable(fieldDesc).Map()
				for mk, mv := range mapData {
					// Convert map key (always string for our schema)
					mapKey := protoreflect.ValueOfString(mk).MapKey()

					// Convert map value based on value type
					valueDesc := fieldDesc.MapValue()
					mvProto, err := m.goValueToProtoValue(mv, valueDesc)
					if err != nil {
						return fmt.Errorf("failed to convert map value for key %s: %w", mk, err)
					}
					mapValue.Set(mapKey, mvProto)
				}
				continue
			}
		}

		// Convert and set the value
		protoValue, err := m.goValueToProtoValue(value, fieldDesc)
		if err != nil {
			return fmt.Errorf("failed to convert field %s: %w", key, err)
		}

		msg.Set(fieldDesc, protoValue)
	}

	return nil
}

// goValueToProtoValue converts a Go value to a Protobuf Value
func (m *Manager) goValueToProtoValue(value interface{}, fieldDesc protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	if value == nil {
		return protoreflect.Value{}, nil
	}

	switch fieldDesc.Kind() {
	case protoreflect.BoolKind:
		if b, ok := value.(bool); ok {
			return protoreflect.ValueOfBool(b), nil
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if i, ok := value.(int32); ok {
			return protoreflect.ValueOfInt32(i), nil
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if i, ok := value.(int64); ok {
			return protoreflect.ValueOfInt64(i), nil
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if i, ok := value.(uint32); ok {
			return protoreflect.ValueOfUint32(i), nil
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if i, ok := value.(uint64); ok {
			return protoreflect.ValueOfUint64(i), nil
		}
	case protoreflect.FloatKind:
		if f, ok := value.(float32); ok {
			return protoreflect.ValueOfFloat32(f), nil
		}
	case protoreflect.DoubleKind:
		if f, ok := value.(float64); ok {
			return protoreflect.ValueOfFloat64(f), nil
		}
	case protoreflect.StringKind:
		if s, ok := value.(string); ok {
			return protoreflect.ValueOfString(s), nil
		}
	case protoreflect.BytesKind:
		if b, ok := value.([]byte); ok {
			return protoreflect.ValueOfBytes(b), nil
		}
	case protoreflect.EnumKind:
		if i, ok := value.(int32); ok {
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(i)), nil
		}
	case protoreflect.MessageKind:
		if nestedMap, ok := value.(map[string]interface{}); ok {
			// Handle nested messages
			nestedMsg := dynamicpb.NewMessage(fieldDesc.Message())
			if err := m.populateProtobufMessage(nestedMsg, nestedMap, fieldDesc.Message()); err != nil {
				return protoreflect.Value{}, err
			}
			return protoreflect.ValueOfMessage(nestedMsg), nil
		}
	}

	return protoreflect.Value{}, fmt.Errorf("unsupported value type %T for field kind %v", value, fieldDesc.Kind())
}

// recordValueToMap converts a RecordValue back to a Go map for encoding
func recordValueToMap(recordValue *schema_pb.RecordValue) map[string]interface{} {
	return recordValueToMapWithAvroContext(recordValue, false)
}

// recordValueToMapWithAvroContext converts a RecordValue back to a Go map for encoding
// with optional Avro union format preservation
func recordValueToMapWithAvroContext(recordValue *schema_pb.RecordValue, preserveAvroUnions bool) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range recordValue.Fields {
		result[key] = schemaValueToGoValueWithAvroContext(value, preserveAvroUnions)
	}

	return result
}

// schemaValueToGoValue converts a schema Value back to a Go value
func schemaValueToGoValue(value *schema_pb.Value) interface{} {
	return schemaValueToGoValueWithAvroContext(value, false)
}

// schemaValueToGoValueWithAvroContext converts a schema Value back to a Go value
// with optional Avro union format preservation
func schemaValueToGoValueWithAvroContext(value *schema_pb.Value, preserveAvroUnions bool) interface{} {
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
			result[i] = schemaValueToGoValueWithAvroContext(item, preserveAvroUnions)
		}
		return result
	case *schema_pb.Value_RecordValue:
		recordMap := recordValueToMapWithAvroContext(v.RecordValue, preserveAvroUnions)

		// Check if this record represents an Avro union
		if preserveAvroUnions && isAvroUnionRecord(v.RecordValue) {
			// Return the union map directly since it's already in the correct format
			return recordMap
		}

		return recordMap
	case *schema_pb.Value_TimestampValue:
		// Convert back to time if needed, or return as int64
		return v.TimestampValue.TimestampMicros
	default:
		// Default to string representation
		return fmt.Sprintf("%v", value)
	}
}

// isAvroUnionRecord checks if a RecordValue represents an Avro union
func isAvroUnionRecord(record *schema_pb.RecordValue) bool {
	// A record represents an Avro union if it has exactly one field
	// and the field name is an Avro type name
	if len(record.Fields) != 1 {
		return false
	}

	for key := range record.Fields {
		return isAvroUnionTypeName(key)
	}

	return false
}

// isAvroUnionTypeName checks if a string is a valid Avro union type name
func isAvroUnionTypeName(name string) bool {
	switch name {
	case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
		return true
	}
	return false
}

// CheckSchemaCompatibility checks if two schemas are compatible
func (m *Manager) CheckSchemaCompatibility(
	oldSchemaStr, newSchemaStr string,
	format Format,
	level CompatibilityLevel,
) (*CompatibilityResult, error) {
	return m.evolutionChecker.CheckCompatibility(oldSchemaStr, newSchemaStr, format, level)
}

// CanEvolveSchema checks if a schema can be evolved for a given subject
func (m *Manager) CanEvolveSchema(
	subject string,
	currentSchemaStr, newSchemaStr string,
	format Format,
) (*CompatibilityResult, error) {
	return m.evolutionChecker.CanEvolve(subject, currentSchemaStr, newSchemaStr, format)
}

// SuggestSchemaEvolution provides suggestions for schema evolution
func (m *Manager) SuggestSchemaEvolution(
	oldSchemaStr, newSchemaStr string,
	format Format,
	level CompatibilityLevel,
) ([]string, error) {
	return m.evolutionChecker.SuggestEvolution(oldSchemaStr, newSchemaStr, format, level)
}

// ValidateSchemaEvolution validates a schema evolution before applying it
func (m *Manager) ValidateSchemaEvolution(
	subject string,
	newSchemaStr string,
	format Format,
) error {
	// Get the current schema for the subject
	currentSchema, err := m.registryClient.GetLatestSchema(subject)
	if err != nil {
		// If no current schema exists, any schema is valid
		return nil
	}

	// Check compatibility
	result, err := m.CanEvolveSchema(subject, currentSchema.Schema, newSchemaStr, format)
	if err != nil {
		return fmt.Errorf("failed to check schema compatibility: %w", err)
	}

	if !result.Compatible {
		return fmt.Errorf("schema evolution is not compatible: %v", result.Issues)
	}

	return nil
}

// GetCompatibilityLevel gets the compatibility level for a subject
func (m *Manager) GetCompatibilityLevel(subject string) CompatibilityLevel {
	return m.evolutionChecker.GetCompatibilityLevel(subject)
}

// SetCompatibilityLevel sets the compatibility level for a subject
func (m *Manager) SetCompatibilityLevel(subject string, level CompatibilityLevel) error {
	return m.evolutionChecker.SetCompatibilityLevel(subject, level)
}

// GetSchemaByID retrieves a schema by its ID
func (m *Manager) GetSchemaByID(schemaID uint32) (*CachedSchema, error) {
	return m.registryClient.GetSchemaByID(schemaID)
}

// GetLatestSchema retrieves the latest schema for a subject
func (m *Manager) GetLatestSchema(subject string) (*CachedSubject, error) {
	return m.registryClient.GetLatestSchema(subject)
}
