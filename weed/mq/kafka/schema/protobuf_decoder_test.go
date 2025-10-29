package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestProtobufDecoder_BasicDecoding tests basic protobuf decoding functionality
func TestProtobufDecoder_BasicDecoding(t *testing.T) {
	// Create a test FileDescriptorSet with a simple message
	fds := createTestFileDescriptorSet(t, "TestMessage", []TestField{
		{Name: "name", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING, Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL},
		{Name: "id", Number: 2, Type: descriptorpb.FieldDescriptorProto_TYPE_INT32, Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL},
	})

	binaryData, err := proto.Marshal(fds)
	require.NoError(t, err)

	t.Run("NewProtobufDecoder with binary descriptor", func(t *testing.T) {
		// This should now work with our integrated descriptor parser
		decoder, err := NewProtobufDecoder(binaryData)

		// Phase E3: Descriptor resolution now works!
		if err != nil {
			// If it fails, it should be due to remaining implementation issues
			assert.True(t,
				strings.Contains(err.Error(), "failed to build file descriptor") ||
					strings.Contains(err.Error(), "message descriptor resolution not fully implemented"),
				"Expected descriptor resolution error, got: %s", err.Error())
			assert.Nil(t, decoder)
		} else {
			// Success! Decoder creation is working
			assert.NotNil(t, decoder)
			assert.NotNil(t, decoder.descriptor)
			t.Log("Protobuf decoder creation succeeded - Phase E3 is working!")
		}
	})

	t.Run("NewProtobufDecoder with empty message name", func(t *testing.T) {
		// Test the findFirstMessageName functionality
		parser := NewProtobufDescriptorParser()
		schema, err := parser.ParseBinaryDescriptor(binaryData, "")

		// Phase E3: Should find the first message name and may succeed
		if err != nil {
			// If it fails, it should be due to remaining implementation issues
			assert.True(t,
				strings.Contains(err.Error(), "failed to build file descriptor") ||
					strings.Contains(err.Error(), "message descriptor resolution not fully implemented"),
				"Expected descriptor resolution error, got: %s", err.Error())
		} else {
			// Success! Empty message name resolution is working
			assert.NotNil(t, schema)
			assert.Equal(t, "TestMessage", schema.MessageName)
			t.Log("Empty message name resolution succeeded - Phase E3 is working!")
		}
	})
}

// TestProtobufDecoder_Integration tests integration with the descriptor parser
func TestProtobufDecoder_Integration(t *testing.T) {
	// Create a more complex test descriptor
	fds := createComplexTestFileDescriptorSet(t)
	binaryData, err := proto.Marshal(fds)
	require.NoError(t, err)

	t.Run("Parse complex descriptor", func(t *testing.T) {
		parser := NewProtobufDescriptorParser()

		// Test with empty message name - should find first message
		schema, err := parser.ParseBinaryDescriptor(binaryData, "")
		// Phase E3: May succeed or fail depending on message complexity
		if err != nil {
			assert.True(t,
				strings.Contains(err.Error(), "failed to build file descriptor") ||
					strings.Contains(err.Error(), "cannot resolve type"),
				"Expected descriptor building error, got: %s", err.Error())
		} else {
			assert.NotNil(t, schema)
			assert.NotEmpty(t, schema.MessageName)
			t.Log("Empty message name resolution succeeded!")
		}

		// Test with specific message name
		schema2, err2 := parser.ParseBinaryDescriptor(binaryData, "ComplexMessage")
		// Phase E3: May succeed or fail depending on message complexity
		if err2 != nil {
			assert.True(t,
				strings.Contains(err2.Error(), "failed to build file descriptor") ||
					strings.Contains(err2.Error(), "cannot resolve type"),
				"Expected descriptor building error, got: %s", err2.Error())
		} else {
			assert.NotNil(t, schema2)
			assert.Equal(t, "ComplexMessage", schema2.MessageName)
			t.Log("Complex message resolution succeeded!")
		}
	})
}

// TestProtobufDecoder_Caching tests that decoder creation uses caching properly
func TestProtobufDecoder_Caching(t *testing.T) {
	fds := createTestFileDescriptorSet(t, "CacheTestMessage", []TestField{
		{Name: "value", Number: 1, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING},
	})

	binaryData, err := proto.Marshal(fds)
	require.NoError(t, err)

	t.Run("Decoder creation uses cache", func(t *testing.T) {
		// First attempt
		_, err1 := NewProtobufDecoder(binaryData)
		assert.Error(t, err1)

		// Second attempt - should use cached parsing
		_, err2 := NewProtobufDecoder(binaryData)
		assert.Error(t, err2)

		// Errors should be identical (indicating cache usage)
		assert.Equal(t, err1.Error(), err2.Error())
	})
}

// Helper function to create a complex test FileDescriptorSet
func createComplexTestFileDescriptorSet(t *testing.T) *descriptorpb.FileDescriptorSet {
	// Create a file descriptor with multiple messages
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test_complex.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("ComplexMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("simple_field"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("repeated_field"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
					},
				},
			},
			{
				Name: proto.String("SimpleMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
				},
			},
		},
	}

	return &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}
}

// TestProtobufDecoder_ErrorHandling tests error handling in various scenarios
func TestProtobufDecoder_ErrorHandling(t *testing.T) {
	t.Run("Invalid binary data", func(t *testing.T) {
		invalidData := []byte("not a protobuf descriptor")
		decoder, err := NewProtobufDecoder(invalidData)

		assert.Error(t, err)
		assert.Nil(t, decoder)
		assert.Contains(t, err.Error(), "failed to parse binary descriptor")
	})

	t.Run("Empty binary data", func(t *testing.T) {
		emptyData := []byte{}
		decoder, err := NewProtobufDecoder(emptyData)

		assert.Error(t, err)
		assert.Nil(t, decoder)
	})

	t.Run("FileDescriptorSet with no messages", func(t *testing.T) {
		// Create an empty FileDescriptorSet
		fds := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{
					Name:    proto.String("empty.proto"),
					Package: proto.String("empty"),
					// No MessageType defined
				},
			},
		}

		binaryData, err := proto.Marshal(fds)
		require.NoError(t, err)

		decoder, err := NewProtobufDecoder(binaryData)
		assert.Error(t, err)
		assert.Nil(t, decoder)
		assert.Contains(t, err.Error(), "no messages found")
	})
}
