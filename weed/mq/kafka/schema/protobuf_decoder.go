package schema

import (
	"encoding/json"
	"fmt"

	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// ProtobufDecoder handles Protobuf schema decoding and conversion to SeaweedMQ format
type ProtobufDecoder struct {
	descriptor protoreflect.MessageDescriptor
	msgType    protoreflect.MessageType
}

// NewProtobufDecoder creates a new Protobuf decoder from a schema descriptor
func NewProtobufDecoder(schemaBytes []byte) (*ProtobufDecoder, error) {
	// Parse the binary descriptor using the descriptor parser
	parser := NewProtobufDescriptorParser()

	// For now, we need to extract the message name from the schema bytes
	// In a real implementation, this would be provided by the Schema Registry
	// For this phase, we'll try to find the first message in the descriptor
	schema, err := parser.ParseBinaryDescriptor(schemaBytes, "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse binary descriptor: %w", err)
	}

	// Create the decoder using the parsed descriptor
	if schema.MessageDescriptor == nil {
		return nil, fmt.Errorf("no message descriptor found in schema")
	}

	return NewProtobufDecoderFromDescriptor(schema.MessageDescriptor), nil
}

// NewProtobufDecoderFromDescriptor creates a Protobuf decoder from a message descriptor
// This is used for testing and when we have pre-built descriptors
func NewProtobufDecoderFromDescriptor(msgDesc protoreflect.MessageDescriptor) *ProtobufDecoder {
	msgType := dynamicpb.NewMessageType(msgDesc)

	return &ProtobufDecoder{
		descriptor: msgDesc,
		msgType:    msgType,
	}
}

// NewProtobufDecoderFromString creates a Protobuf decoder from a schema string
// This parses text .proto format from Schema Registry
func NewProtobufDecoderFromString(schemaStr string) (*ProtobufDecoder, error) {
	// Use protoparse to parse the text .proto schema
	parser := protoparse.Parser{
		Accessor: protoparse.FileContentsFromMap(map[string]string{
			"schema.proto": schemaStr,
		}),
	}

	// Parse the schema
	fileDescs, err := parser.ParseFiles("schema.proto")
	if err != nil {
		return nil, fmt.Errorf("failed to parse .proto schema: %w", err)
	}

	if len(fileDescs) == 0 {
		return nil, fmt.Errorf("no file descriptors found in schema")
	}

	fileDesc := fileDescs[0]

	// Convert to protoreflect FileDescriptor
	fileDescProto := fileDesc.AsFileDescriptorProto()

	// Create a FileDescriptor from the proto
	protoFileDesc, err := protodesc.NewFile(fileDescProto, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptor: %w", err)
	}

	// Find the first message in the file
	messages := protoFileDesc.Messages()
	if messages.Len() == 0 {
		return nil, fmt.Errorf("no message types found in schema")
	}

	// Get the first message descriptor
	msgDesc := messages.Get(0)

	return NewProtobufDecoderFromDescriptor(msgDesc), nil
}

// Decode decodes Protobuf binary data to a Go map representation
// Also supports JSON fallback for compatibility with producers that don't yet support Protobuf binary
func (pd *ProtobufDecoder) Decode(data []byte) (map[string]interface{}, error) {
	// Create a new message instance
	msg := pd.msgType.New()

	// Try to unmarshal as Protobuf binary first
	if err := proto.Unmarshal(data, msg.Interface()); err != nil {
		// Fallback: Try JSON decoding (for compatibility with producers that send JSON)
		var jsonMap map[string]interface{}
		if jsonErr := json.Unmarshal(data, &jsonMap); jsonErr == nil {
			// Successfully decoded as JSON - return it
			// Note: This is a compatibility fallback, proper Protobuf binary is preferred
			return jsonMap, nil
		}
		// Both failed - return the original Protobuf error
		return nil, fmt.Errorf("failed to unmarshal Protobuf data: %w", err)
	}

	// Convert to map representation
	return pd.messageToMap(msg), nil
}

// DecodeToRecordValue decodes Protobuf data directly to SeaweedMQ RecordValue
func (pd *ProtobufDecoder) DecodeToRecordValue(data []byte) (*schema_pb.RecordValue, error) {
	msgMap, err := pd.Decode(data)
	if err != nil {
		return nil, err
	}

	return MapToRecordValue(msgMap), nil
}

// InferRecordType infers a SeaweedMQ RecordType from the Protobuf descriptor
func (pd *ProtobufDecoder) InferRecordType() (*schema_pb.RecordType, error) {
	return pd.descriptorToRecordType(pd.descriptor), nil
}

// messageToMap converts a Protobuf message to a Go map
func (pd *ProtobufDecoder) messageToMap(msg protoreflect.Message) map[string]interface{} {
	result := make(map[string]interface{})

	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		fieldName := string(fd.Name())
		result[fieldName] = pd.valueToInterface(fd, v)
		return true
	})

	return result
}

// valueToInterface converts a Protobuf value to a Go interface{}
func (pd *ProtobufDecoder) valueToInterface(fd protoreflect.FieldDescriptor, v protoreflect.Value) interface{} {
	if fd.IsList() {
		// Handle repeated fields
		list := v.List()
		result := make([]interface{}, list.Len())
		for i := 0; i < list.Len(); i++ {
			result[i] = pd.scalarValueToInterface(fd, list.Get(i))
		}
		return result
	}

	if fd.IsMap() {
		// Handle map fields
		mapVal := v.Map()
		result := make(map[string]interface{})
		mapVal.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			keyStr := fmt.Sprintf("%v", k.Interface())
			result[keyStr] = pd.scalarValueToInterface(fd.MapValue(), v)
			return true
		})
		return result
	}

	return pd.scalarValueToInterface(fd, v)
}

// scalarValueToInterface converts a scalar Protobuf value to Go interface{}
func (pd *ProtobufDecoder) scalarValueToInterface(fd protoreflect.FieldDescriptor, v protoreflect.Value) interface{} {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(v.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return v.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint32(v.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return v.Uint()
	case protoreflect.FloatKind:
		return float32(v.Float())
	case protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return v.Bytes()
	case protoreflect.EnumKind:
		return int32(v.Enum())
	case protoreflect.MessageKind:
		// Handle nested messages
		nestedMsg := v.Message()
		return pd.messageToMap(nestedMsg)
	default:
		// Fallback to string representation
		return fmt.Sprintf("%v", v.Interface())
	}
}

// descriptorToRecordType converts a Protobuf descriptor to SeaweedMQ RecordType
func (pd *ProtobufDecoder) descriptorToRecordType(desc protoreflect.MessageDescriptor) *schema_pb.RecordType {
	fields := make([]*schema_pb.Field, 0, desc.Fields().Len())

	for i := 0; i < desc.Fields().Len(); i++ {
		fd := desc.Fields().Get(i)

		field := &schema_pb.Field{
			Name:       string(fd.Name()),
			FieldIndex: int32(fd.Number() - 1), // Protobuf field numbers start at 1
			Type:       pd.fieldDescriptorToType(fd),
			IsRequired: fd.Cardinality() == protoreflect.Required,
			IsRepeated: fd.IsList(),
		}

		fields = append(fields, field)
	}

	return &schema_pb.RecordType{
		Fields: fields,
	}
}

// fieldDescriptorToType converts a Protobuf field descriptor to SeaweedMQ Type
func (pd *ProtobufDecoder) fieldDescriptorToType(fd protoreflect.FieldDescriptor) *schema_pb.Type {
	if fd.IsList() {
		// Handle repeated fields
		elementType := pd.scalarKindToType(fd.Kind(), fd.Message())
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ListType{
				ListType: &schema_pb.ListType{
					ElementType: elementType,
				},
			},
		}
	}

	if fd.IsMap() {
		// Handle map fields - for simplicity, treat as record with key/value fields
		keyType := pd.scalarKindToType(fd.MapKey().Kind(), nil)
		valueType := pd.scalarKindToType(fd.MapValue().Kind(), fd.MapValue().Message())

		mapRecordType := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name:       "key",
					FieldIndex: 0,
					Type:       keyType,
					IsRequired: true,
				},
				{
					Name:       "value",
					FieldIndex: 1,
					Type:       valueType,
					IsRequired: false,
				},
			},
		}

		return &schema_pb.Type{
			Kind: &schema_pb.Type_RecordType{
				RecordType: mapRecordType,
			},
		}
	}

	return pd.scalarKindToType(fd.Kind(), fd.Message())
}

// scalarKindToType converts a Protobuf kind to SeaweedMQ scalar type
func (pd *ProtobufDecoder) scalarKindToType(kind protoreflect.Kind, msgDesc protoreflect.MessageDescriptor) *schema_pb.Type {
	switch kind {
	case protoreflect.BoolKind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BOOL,
			},
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT32,
			},
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT64,
			},
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT32, // Map uint32 to int32 for simplicity
			},
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT64, // Map uint64 to int64 for simplicity
			},
		}
	case protoreflect.FloatKind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_FLOAT,
			},
		}
	case protoreflect.DoubleKind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_DOUBLE,
			},
		}
	case protoreflect.StringKind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}
	case protoreflect.BytesKind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BYTES,
			},
		}
	case protoreflect.EnumKind:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT32, // Enums as int32
			},
		}
	case protoreflect.MessageKind:
		if msgDesc != nil {
			// Handle nested messages
			nestedRecordType := pd.descriptorToRecordType(msgDesc)
			return &schema_pb.Type{
				Kind: &schema_pb.Type_RecordType{
					RecordType: nestedRecordType,
				},
			}
		}
		fallthrough
	default:
		// Default to string for unknown types
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}
	}
}
