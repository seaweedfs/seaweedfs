package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestProtobufDescriptorParser_BasicParsing tests basic descriptor parsing functionality
func TestProtobufDescriptorParser_BasicParsing(t *testing.T) {
	parser := NewProtobufDescriptorParser()

	t.Run("Parse Simple Message Descriptor", func(t *testing.T) {
		// Create a simple FileDescriptorSet for testing
		fds := createTestFileDescriptorSet(t, "TestMessage", []TestField{
			{Name: "id", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_INT32},
			{Name: "name", Number: 2, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING},
		})

		binaryData, err := proto.Marshal(fds)
		require.NoError(t, err)

		// Parse the descriptor
		_, err = parser.ParseBinaryDescriptor(binaryData, "TestMessage")
		
		// In Phase E1, this should return an error indicating incomplete implementation
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message descriptor resolution not fully implemented")
	})

	t.Run("Parse Complex Message Descriptor", func(t *testing.T) {
		// Create a more complex FileDescriptorSet
		fds := createTestFileDescriptorSet(t, "ComplexMessage", []TestField{
			{Name: "user_id", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING},
			{Name: "metadata", Number: 2, Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, TypeName: "Metadata"},
			{Name: "tags", Number: 3, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING, Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED},
		})

		binaryData, err := proto.Marshal(fds)
		require.NoError(t, err)

		// Parse the descriptor
		_, err = parser.ParseBinaryDescriptor(binaryData, "ComplexMessage")
		
		// Should find the message but fail on descriptor resolution
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message descriptor resolution not fully implemented")
	})

	t.Run("Cache Functionality", func(t *testing.T) {
		// Create a fresh parser for this test to avoid interference
		freshParser := NewProtobufDescriptorParser()
		
		fds := createTestFileDescriptorSet(t, "CacheTest", []TestField{
			{Name: "value", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING},
		})

		binaryData, err := proto.Marshal(fds)
		require.NoError(t, err)

		// First parse
		_, err1 := freshParser.ParseBinaryDescriptor(binaryData, "CacheTest")
		assert.Error(t, err1)

		// Second parse (should use cache)
		_, err2 := freshParser.ParseBinaryDescriptor(binaryData, "CacheTest")
		assert.Error(t, err2)

		// Errors should be identical (indicating cache usage)
		assert.Equal(t, err1.Error(), err2.Error())

		// Check cache stats - should be 1 since descriptor was cached even though resolution failed
		stats := freshParser.GetCacheStats()
		assert.Equal(t, 1, stats["cached_descriptors"])
	})
}

// TestProtobufDescriptorParser_Validation tests descriptor validation
func TestProtobufDescriptorParser_Validation(t *testing.T) {
	parser := NewProtobufDescriptorParser()

	t.Run("Invalid Binary Data", func(t *testing.T) {
		invalidData := []byte("not a protobuf descriptor")
		
		_, err := parser.ParseBinaryDescriptor(invalidData, "TestMessage")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal FileDescriptorSet")
	})

	t.Run("Empty FileDescriptorSet", func(t *testing.T) {
		emptyFds := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{},
		}

		binaryData, err := proto.Marshal(emptyFds)
		require.NoError(t, err)

		_, err = parser.ParseBinaryDescriptor(binaryData, "TestMessage")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "FileDescriptorSet contains no files")
	})

	t.Run("FileDescriptor Without Name", func(t *testing.T) {
		invalidFds := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{
					// Missing Name field
					Package: proto.String("test.package"),
				},
			},
		}

		binaryData, err := proto.Marshal(invalidFds)
		require.NoError(t, err)

		_, err = parser.ParseBinaryDescriptor(binaryData, "TestMessage")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file descriptor 0 has no name")
	})

	t.Run("FileDescriptor Without Package", func(t *testing.T) {
		invalidFds := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{
					Name: proto.String("test.proto"),
					// Missing Package field
				},
			},
		}

		binaryData, err := proto.Marshal(invalidFds)
		require.NoError(t, err)

		_, err = parser.ParseBinaryDescriptor(binaryData, "TestMessage")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file descriptor test.proto has no package")
	})
}

// TestProtobufDescriptorParser_MessageSearch tests message finding functionality
func TestProtobufDescriptorParser_MessageSearch(t *testing.T) {
	parser := NewProtobufDescriptorParser()

	t.Run("Message Not Found", func(t *testing.T) {
		fds := createTestFileDescriptorSet(t, "ExistingMessage", []TestField{
			{Name: "field1", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING},
		})

		binaryData, err := proto.Marshal(fds)
		require.NoError(t, err)

		_, err = parser.ParseBinaryDescriptor(binaryData, "NonExistentMessage")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message NonExistentMessage not found")
	})

	t.Run("Nested Message Search", func(t *testing.T) {
		// Create FileDescriptorSet with nested messages
		fds := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{
					Name:    proto.String("test.proto"),
					Package: proto.String("test.package"),
					MessageType: []*descriptorpb.DescriptorProto{
						{
							Name: proto.String("OuterMessage"),
							NestedType: []*descriptorpb.DescriptorProto{
								{
									Name: proto.String("NestedMessage"),
									Field: []*descriptorpb.FieldDescriptorProto{
										{
											Name:   proto.String("nested_field"),
											Number: proto.Int32(1),
											Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
										},
									},
								},
							},
						},
					},
				},
			},
		}

		binaryData, err := proto.Marshal(fds)
		require.NoError(t, err)

		_, err = parser.ParseBinaryDescriptor(binaryData, "NestedMessage")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nested message descriptor resolution not fully implemented")
	})
}

// TestProtobufDescriptorParser_Dependencies tests dependency extraction
func TestProtobufDescriptorParser_Dependencies(t *testing.T) {
	parser := NewProtobufDescriptorParser()

	t.Run("Extract Dependencies", func(t *testing.T) {
		// Create FileDescriptorSet with dependencies
		fds := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{
					Name:    proto.String("main.proto"),
					Package: proto.String("main.package"),
					Dependency: []string{
						"google/protobuf/timestamp.proto",
						"common/types.proto",
					},
					MessageType: []*descriptorpb.DescriptorProto{
						{
							Name: proto.String("MainMessage"),
							Field: []*descriptorpb.FieldDescriptorProto{
								{
									Name:   proto.String("id"),
									Number: proto.Int32(1),
									Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								},
							},
						},
					},
				},
			},
		}

		_, err := proto.Marshal(fds)
		require.NoError(t, err)

		// Parse and check dependencies (even though parsing fails, we can test dependency extraction)
		dependencies := parser.extractDependencies(fds)
		assert.Len(t, dependencies, 2)
		assert.Contains(t, dependencies, "google/protobuf/timestamp.proto")
		assert.Contains(t, dependencies, "common/types.proto")
	})
}

// TestProtobufSchema_Methods tests ProtobufSchema methods
func TestProtobufSchema_Methods(t *testing.T) {
	// Create a basic schema for testing
	fds := createTestFileDescriptorSet(t, "TestSchema", []TestField{
		{Name: "field1", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING},
	})

	schema := &ProtobufSchema{
		FileDescriptorSet: fds,
		MessageDescriptor: nil, // Not implemented in Phase E1
		MessageName:       "TestSchema",
		PackageName:       "test.package",
		Dependencies:      []string{"common.proto"},
	}

	t.Run("GetMessageFields Not Implemented", func(t *testing.T) {
		_, err := schema.GetMessageFields()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field information extraction not implemented in Phase E1")
	})

	t.Run("GetFieldByName Not Implemented", func(t *testing.T) {
		_, err := schema.GetFieldByName("field1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field information extraction not implemented in Phase E1")
	})

	t.Run("GetFieldByNumber Not Implemented", func(t *testing.T) {
		_, err := schema.GetFieldByNumber(1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field information extraction not implemented in Phase E1")
	})

	t.Run("ValidateMessage Not Implemented", func(t *testing.T) {
		err := schema.ValidateMessage([]byte("test message"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message validation not implemented in Phase E1")
	})
}

// TestProtobufDescriptorParser_CacheManagement tests cache management
func TestProtobufDescriptorParser_CacheManagement(t *testing.T) {
	parser := NewProtobufDescriptorParser()

	// Add some entries to cache
	fds1 := createTestFileDescriptorSet(t, "Message1", []TestField{
		{Name: "field1", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING},
	})
	fds2 := createTestFileDescriptorSet(t, "Message2", []TestField{
		{Name: "field2", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_INT32},
	})

	binaryData1, _ := proto.Marshal(fds1)
	binaryData2, _ := proto.Marshal(fds2)

	// Parse both (will fail but add to cache)
	parser.ParseBinaryDescriptor(binaryData1, "Message1")
	parser.ParseBinaryDescriptor(binaryData2, "Message2")

	// Check cache has entries (descriptors cached even though resolution failed)
	stats := parser.GetCacheStats()
	assert.Equal(t, 2, stats["cached_descriptors"])

	// Clear cache
	parser.ClearCache()

	// Check cache is empty
	stats = parser.GetCacheStats()
	assert.Equal(t, 0, stats["cached_descriptors"])
}

// Helper types and functions for testing

type TestField struct {
	Name     string
	Number   int32
	Type     descriptorpb.FieldDescriptorProto_Type
	Label    descriptorpb.FieldDescriptorProto_Label
	TypeName string
}

func createTestFileDescriptorSet(t *testing.T, messageName string, fields []TestField) *descriptorpb.FileDescriptorSet {
	// Create field descriptors
	fieldDescriptors := make([]*descriptorpb.FieldDescriptorProto, len(fields))
	for i, field := range fields {
		fieldDesc := &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(field.Name),
			Number: proto.Int32(field.Number),
			Type:   field.Type.Enum(),
		}

		if field.Label != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
			fieldDesc.Label = field.Label.Enum()
		}

		if field.TypeName != "" {
			fieldDesc.TypeName = proto.String(field.TypeName)
		}

		fieldDescriptors[i] = fieldDesc
	}

	// Create message descriptor
	messageDesc := &descriptorpb.DescriptorProto{
		Name:  proto.String(messageName),
		Field: fieldDescriptors,
	}

	// Create file descriptor
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("test.proto"),
		Package:     proto.String("test.package"),
		MessageType: []*descriptorpb.DescriptorProto{messageDesc},
	}

	// Create FileDescriptorSet
	return &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}
}
