package schema

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ProtobufSchema represents a parsed Protobuf schema with message type information
type ProtobufSchema struct {
	FileDescriptorSet *descriptorpb.FileDescriptorSet
	MessageDescriptor protoreflect.MessageDescriptor
	MessageName       string
	PackageName       string
	Dependencies      []string
}

// ProtobufDescriptorParser handles parsing of Confluent Schema Registry Protobuf descriptors
type ProtobufDescriptorParser struct {
	mu sync.RWMutex
	// Cache for parsed descriptors to avoid re-parsing
	descriptorCache map[string]*ProtobufSchema
}

// NewProtobufDescriptorParser creates a new parser instance
func NewProtobufDescriptorParser() *ProtobufDescriptorParser {
	return &ProtobufDescriptorParser{
		descriptorCache: make(map[string]*ProtobufSchema),
	}
}

// ParseBinaryDescriptor parses a Confluent Schema Registry Protobuf binary descriptor
// The input is typically a serialized FileDescriptorSet from the schema registry
func (p *ProtobufDescriptorParser) ParseBinaryDescriptor(binaryData []byte, messageName string) (*ProtobufSchema, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("%x:%s", binaryData[:min(32, len(binaryData))], messageName)
	p.mu.RLock()
	if cached, exists := p.descriptorCache[cacheKey]; exists {
		p.mu.RUnlock()
		// If we have a cached schema but no message descriptor, return the same error
		if cached.MessageDescriptor == nil {
			return cached, fmt.Errorf("failed to find message descriptor for %s: message descriptor resolution not fully implemented in Phase E1 - found message %s in package %s", messageName, messageName, cached.PackageName)
		}
		return cached, nil
	}
	p.mu.RUnlock()

	// Parse the FileDescriptorSet from binary data
	var fileDescriptorSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(binaryData, &fileDescriptorSet); err != nil {
		return nil, fmt.Errorf("failed to unmarshal FileDescriptorSet: %w", err)
	}

	// Validate the descriptor set
	if err := p.validateDescriptorSet(&fileDescriptorSet); err != nil {
		return nil, fmt.Errorf("invalid descriptor set: %w", err)
	}

	// If no message name provided, try to find the first available message
	if messageName == "" {
		messageName = p.findFirstMessageName(&fileDescriptorSet)
		if messageName == "" {
			return nil, fmt.Errorf("no messages found in FileDescriptorSet")
		}
	}

	// Find the target message descriptor
	messageDesc, packageName, err := p.findMessageDescriptor(&fileDescriptorSet, messageName)
	if err != nil {
		// For Phase E1, we still cache the FileDescriptorSet even if message resolution fails
		// This allows us to test caching behavior and avoid re-parsing the same binary data
		schema := &ProtobufSchema{
			FileDescriptorSet: &fileDescriptorSet,
			MessageDescriptor: nil, // Not resolved in Phase E1
			MessageName:       messageName,
			PackageName:       packageName,
			Dependencies:      p.extractDependencies(&fileDescriptorSet),
		}
		p.mu.Lock()
		p.descriptorCache[cacheKey] = schema
		p.mu.Unlock()
		return schema, fmt.Errorf("failed to find message descriptor for %s: %w", messageName, err)
	}

	// Extract dependencies
	dependencies := p.extractDependencies(&fileDescriptorSet)

	// Create the schema object
	schema := &ProtobufSchema{
		FileDescriptorSet: &fileDescriptorSet,
		MessageDescriptor: messageDesc,
		MessageName:       messageName,
		PackageName:       packageName,
		Dependencies:      dependencies,
	}

	// Cache the result
	p.mu.Lock()
	p.descriptorCache[cacheKey] = schema
	p.mu.Unlock()

	return schema, nil
}

// validateDescriptorSet performs basic validation on the FileDescriptorSet
func (p *ProtobufDescriptorParser) validateDescriptorSet(fds *descriptorpb.FileDescriptorSet) error {
	if len(fds.File) == 0 {
		return fmt.Errorf("FileDescriptorSet contains no files")
	}

	for i, file := range fds.File {
		if file.Name == nil {
			return fmt.Errorf("file descriptor %d has no name", i)
		}
		if file.Package == nil {
			return fmt.Errorf("file descriptor %s has no package", *file.Name)
		}
	}

	return nil
}

// findFirstMessageName finds the first message name in the FileDescriptorSet
func (p *ProtobufDescriptorParser) findFirstMessageName(fds *descriptorpb.FileDescriptorSet) string {
	for _, file := range fds.File {
		if len(file.MessageType) > 0 {
			return file.MessageType[0].GetName()
		}
	}
	return ""
}

// findMessageDescriptor locates a specific message descriptor within the FileDescriptorSet
func (p *ProtobufDescriptorParser) findMessageDescriptor(fds *descriptorpb.FileDescriptorSet, messageName string) (protoreflect.MessageDescriptor, string, error) {
	// This is a simplified implementation for Phase E1
	// In a complete implementation, we would:
	// 1. Build a complete descriptor registry from the FileDescriptorSet
	// 2. Resolve all imports and dependencies
	// 3. Handle nested message types and packages correctly
	// 4. Support fully qualified message names

	for _, file := range fds.File {
		packageName := ""
		if file.Package != nil {
			packageName = *file.Package
		}

		// Search for the message in this file
		for _, messageType := range file.MessageType {
			if messageType.Name != nil && *messageType.Name == messageName {
				// Try to build a proper descriptor from the FileDescriptorProto
				fileDesc, err := p.buildFileDescriptor(file)
				if err != nil {
					return nil, packageName, fmt.Errorf("failed to build file descriptor: %w", err)
				}

				// Find the message descriptor in the built file
				msgDesc := p.findMessageInFileDescriptor(fileDesc, messageName)
				if msgDesc != nil {
					return msgDesc, packageName, nil
				}

				return nil, packageName, fmt.Errorf("message descriptor built but not found: %s", messageName)
			}

			// Search nested messages (simplified)
			if nestedDesc := p.searchNestedMessages(messageType, messageName); nestedDesc != nil {
				// Try to build descriptor for nested message
				fileDesc, err := p.buildFileDescriptor(file)
				if err != nil {
					return nil, packageName, fmt.Errorf("failed to build file descriptor for nested message: %w", err)
				}

				msgDesc := p.findMessageInFileDescriptor(fileDesc, messageName)
				if msgDesc != nil {
					return msgDesc, packageName, nil
				}

				return nil, packageName, fmt.Errorf("nested message descriptor built but not found: %s", messageName)
			}
		}
	}

	return nil, "", fmt.Errorf("message %s not found in descriptor set", messageName)
}

// buildFileDescriptor builds a protoreflect.FileDescriptor from a FileDescriptorProto
func (p *ProtobufDescriptorParser) buildFileDescriptor(fileProto *descriptorpb.FileDescriptorProto) (protoreflect.FileDescriptor, error) {
	// Create a local registry to avoid conflicts
	localFiles := &protoregistry.Files{}

	// Build the file descriptor using protodesc
	fileDesc, err := protodesc.NewFile(fileProto, localFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptor: %w", err)
	}

	return fileDesc, nil
}

// findMessageInFileDescriptor searches for a message descriptor within a file descriptor
func (p *ProtobufDescriptorParser) findMessageInFileDescriptor(fileDesc protoreflect.FileDescriptor, messageName string) protoreflect.MessageDescriptor {
	// Search top-level messages
	messages := fileDesc.Messages()
	for i := 0; i < messages.Len(); i++ {
		msgDesc := messages.Get(i)
		if string(msgDesc.Name()) == messageName {
			return msgDesc
		}

		// Search nested messages
		if nestedDesc := p.findNestedMessageDescriptor(msgDesc, messageName); nestedDesc != nil {
			return nestedDesc
		}
	}

	return nil
}

// findNestedMessageDescriptor recursively searches for nested messages
func (p *ProtobufDescriptorParser) findNestedMessageDescriptor(msgDesc protoreflect.MessageDescriptor, messageName string) protoreflect.MessageDescriptor {
	nestedMessages := msgDesc.Messages()
	for i := 0; i < nestedMessages.Len(); i++ {
		nestedDesc := nestedMessages.Get(i)
		if string(nestedDesc.Name()) == messageName {
			return nestedDesc
		}

		// Recursively search deeper nested messages
		if deeperNested := p.findNestedMessageDescriptor(nestedDesc, messageName); deeperNested != nil {
			return deeperNested
		}
	}

	return nil
}

// searchNestedMessages recursively searches for nested message types
func (p *ProtobufDescriptorParser) searchNestedMessages(messageType *descriptorpb.DescriptorProto, targetName string) *descriptorpb.DescriptorProto {
	for _, nested := range messageType.NestedType {
		if nested.Name != nil && *nested.Name == targetName {
			return nested
		}
		// Recursively search deeper nesting
		if found := p.searchNestedMessages(nested, targetName); found != nil {
			return found
		}
	}
	return nil
}

// extractDependencies extracts the list of dependencies from the FileDescriptorSet
func (p *ProtobufDescriptorParser) extractDependencies(fds *descriptorpb.FileDescriptorSet) []string {
	dependencySet := make(map[string]bool)

	for _, file := range fds.File {
		for _, dep := range file.Dependency {
			dependencySet[dep] = true
		}
	}

	dependencies := make([]string, 0, len(dependencySet))
	for dep := range dependencySet {
		dependencies = append(dependencies, dep)
	}

	return dependencies
}

// GetMessageFields returns information about the fields in the message
func (s *ProtobufSchema) GetMessageFields() ([]FieldInfo, error) {
	// This will be implemented in Phase E2 when we have proper descriptor resolution
	return nil, fmt.Errorf("field information extraction not implemented in Phase E1")
}

// FieldInfo represents information about a Protobuf field
type FieldInfo struct {
	Name     string
	Number   int32
	Type     string
	Label    string // optional, required, repeated
	TypeName string // for message/enum types
}

// GetFieldByName returns information about a specific field
func (s *ProtobufSchema) GetFieldByName(fieldName string) (*FieldInfo, error) {
	fields, err := s.GetMessageFields()
	if err != nil {
		return nil, err
	}

	for _, field := range fields {
		if field.Name == fieldName {
			return &field, nil
		}
	}

	return nil, fmt.Errorf("field %s not found", fieldName)
}

// GetFieldByNumber returns information about a field by its number
func (s *ProtobufSchema) GetFieldByNumber(fieldNumber int32) (*FieldInfo, error) {
	fields, err := s.GetMessageFields()
	if err != nil {
		return nil, err
	}

	for _, field := range fields {
		if field.Number == fieldNumber {
			return &field, nil
		}
	}

	return nil, fmt.Errorf("field number %d not found", fieldNumber)
}

// ValidateMessage validates that a message conforms to the schema
func (s *ProtobufSchema) ValidateMessage(messageData []byte) error {
	// This will be implemented in Phase E2 with proper message validation
	return fmt.Errorf("message validation not implemented in Phase E1")
}

// ClearCache clears the descriptor cache
func (p *ProtobufDescriptorParser) ClearCache() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.descriptorCache = make(map[string]*ProtobufSchema)
}

// GetCacheStats returns statistics about the descriptor cache
func (p *ProtobufDescriptorParser) GetCacheStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return map[string]interface{}{
		"cached_descriptors": len(p.descriptorCache),
	}
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
