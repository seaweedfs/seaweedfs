# Kafka Integration Testing - Refactored Structure

This document describes the refactored Kafka integration testing structure for SeaweedFS.

## 🎯 **Refactoring Goals**

- **Better Organization**: Clear separation of unit, integration, and e2e tests
- **Reduced Duplication**: Common utilities and helpers extracted
- **Improved Maintainability**: Consistent patterns and naming conventions
- **Enhanced Readability**: Well-documented and structured code
- **Easier Development**: Simplified test creation and debugging

## 📁 **New Directory Structure**

```
test/kafka/
├── internal/testutil/          # Common test utilities (NEW)
│   ├── gateway.go             # Gateway server utilities
│   ├── clients.go             # Kafka client wrappers
│   ├── messages.go            # Message generation and validation
│   ├── docker.go              # Docker environment helpers
│   └── assertions.go          # Custom test assertions
├── unit/                      # Unit tests (NEW)
│   └── gateway_test.go        # Gateway unit tests
├── integration/               # Integration tests (NEW)
│   ├── client_compatibility_test.go  # Client compatibility tests
│   ├── consumer_groups_test.go       # Consumer group tests
│   └── docker_test.go               # Docker integration tests
├── e2e/                       # End-to-end tests (NEW)
│   ├── comprehensive_test.go         # Comprehensive E2E scenarios
│   └── offset_management_test.go     # Offset management E2E tests
├── cmd/setup/                 # Test setup utilities (EXISTING)
├── scripts/                   # Helper scripts (EXISTING)
├── docker-compose.yml         # Docker setup (EXISTING)
├── Makefile.new              # Refactored Makefile (NEW)
└── README_REFACTORED.md      # This documentation (NEW)
```

## 🛠 **Common Test Utilities**

### **Gateway Utilities** (`internal/testutil/gateway.go`)

```go
// Create and manage test gateway servers
gateway := testutil.NewGatewayTestServer(t, testutil.GatewayOptions{})
defer gateway.CleanupAndClose()

addr := gateway.StartAndWait()
gateway.AddTestTopic("my-topic")
```

### **Client Utilities** (`internal/testutil/clients.go`)

```go
// Kafka-go client wrapper
kafkaGoClient := testutil.NewKafkaGoClient(t, addr)
err := kafkaGoClient.ProduceMessages(topic, messages)
consumed, err := kafkaGoClient.ConsumeMessages(topic, count)

// Sarama client wrapper
saramaClient := testutil.NewSaramaClient(t, addr)
err := saramaClient.CreateTopic(topic, partitions, replicationFactor)
err := saramaClient.ProduceMessages(topic, stringMessages)
```

### **Message Utilities** (`internal/testutil/messages.go`)

```go
// Generate test messages
msgGen := testutil.NewMessageGenerator()
kafkaGoMessages := msgGen.GenerateKafkaGoMessages(5)
stringMessages := msgGen.GenerateStringMessages(5)

// Generate unique names
topic := testutil.GenerateUniqueTopicName("test-topic")
groupID := testutil.GenerateUniqueGroupID("test-group")

// Validate message content
err := testutil.ValidateKafkaGoMessageContent(expected, actual)
err := testutil.ValidateMessageContent(expectedStrings, actualStrings)
```

### **Docker Utilities** (`internal/testutil/docker.go`)

```go
// Handle Docker environment
env := testutil.NewDockerEnvironment(t)
env.SkipIfNotAvailable(t)
env.RequireKafka(t)
env.RequireGateway(t)
```

### **Custom Assertions** (`internal/testutil/assertions.go`)

```go
// Enhanced assertions with better error messages
testutil.AssertNoError(t, err, "Failed to create topic")
testutil.AssertEqual(t, expected, actual, "Message count mismatch")
testutil.AssertEventually(t, assertion, timeout, interval, "Condition not met")
```

## 📋 **Test Categories**

### **Unit Tests** (`unit/`)
- Test individual components in isolation
- No external dependencies (Docker, real Kafka)
- Fast execution
- Focus on gateway functionality, protocol handling

### **Integration Tests** (`integration/`)
- Test component interactions
- May use Docker environment
- Test client compatibility, consumer groups
- Focus on SeaweedFS Kafka Gateway integration

### **End-to-End Tests** (`e2e/`)
- Test complete workflows
- Full environment setup required
- Test real-world scenarios
- Focus on user-facing functionality

## 🚀 **Usage Examples**

### **Running Tests**

```bash
# Run all tests
make test

# Run specific categories
make test-unit
make test-integration
make test-e2e

# Run Docker-based tests
make test-docker

# Run client-specific tests
make test-sarama
make test-kafka-go
```

### **Development Workflow**

```bash
# Start development environment
make dev-kafka        # Just Kafka ecosystem
make dev-seaweedfs     # Just SeaweedFS
make dev-gateway       # Full gateway setup

# Quick development test
make dev-test

# Monitor services
make status
make logs-gateway
```

### **Writing New Tests**

#### **Unit Test Example**

```go
package unit

import (
    "testing"
    "github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

func TestMyFeature(t *testing.T) {
    gateway := testutil.NewGatewayTestServer(t, testutil.GatewayOptions{})
    defer gateway.CleanupAndClose()
    
    addr := gateway.StartAndWait()
    
    // Test implementation
    client := testutil.NewKafkaGoClient(t, addr)
    // ... test logic
}
```

#### **Integration Test Example**

```go
package integration

func TestClientCompatibility(t *testing.T) {
    gateway := testutil.NewGatewayTestServer(t, testutil.GatewayOptions{})
    defer gateway.CleanupAndClose()
    
    addr := gateway.StartAndWait()
    topic := testutil.GenerateUniqueTopicName("compatibility-test")
    gateway.AddTestTopic(topic)
    
    // Test cross-client compatibility
    kafkaGoClient := testutil.NewKafkaGoClient(t, addr)
    saramaClient := testutil.NewSaramaClient(t, addr)
    // ... test logic
}
```

#### **E2E Test Example**

```go
package e2e

func TestCompleteWorkflow(t *testing.T) {
    env := testutil.NewDockerEnvironment(t)
    env.SkipIfNotAvailable(t)
    
    // Test complete end-to-end workflow
    // ... test logic
}
```

## 🔄 **Migration Guide**

### **From Old Structure to New Structure**

1. **Extract Common Code**: Move repeated gateway setup, client creation, and message generation to `testutil` packages

2. **Categorize Tests**: Move tests to appropriate directories:
   - `*_debug_test.go` → Remove or convert to proper tests
   - Basic functionality → `unit/`
   - Client interactions → `integration/`
   - Complete workflows → `e2e/`

3. **Update Imports**: Change imports to use new `testutil` packages

4. **Standardize Naming**: Use consistent naming patterns:
   - `TestFeatureName` for test functions
   - `testFeatureSpecificCase` for helper functions
   - Descriptive test names that explain what is being tested

5. **Use New Utilities**: Replace custom setup code with `testutil` functions

### **Example Migration**

**Before (old structure):**
```go
func TestSaramaBasic(t *testing.T) {
    // 50 lines of gateway setup, client creation, message generation
    srv := gateway.NewTestServer(gateway.Options{Listen: ":0"})
    // ... lots of boilerplate
}
```

**After (new structure):**
```go
func TestSaramaBasicFunctionality(t *testing.T) {
    gateway := testutil.NewGatewayTestServer(t, testutil.GatewayOptions{})
    defer gateway.CleanupAndClose()
    
    addr := gateway.StartAndWait()
    client := testutil.NewSaramaClient(t, addr)
    // ... focus on actual test logic
}
```

## 📊 **Benefits of Refactoring**

### **Before Refactoring**
- ❌ 40+ test files in single directory
- ❌ Massive code duplication
- ❌ Inconsistent naming and patterns
- ❌ Debug tests mixed with real tests
- ❌ Hard to find and run specific test types
- ❌ Difficult to maintain and extend

### **After Refactoring**
- ✅ Clear organization by test type
- ✅ Reusable test utilities
- ✅ Consistent patterns and naming
- ✅ Separated debug/development tests
- ✅ Easy to run specific test categories
- ✅ Maintainable and extensible structure

## 🎯 **Next Steps**

1. **Complete Migration**: Move remaining tests to new structure
2. **Remove Debug Tests**: Clean up or properly organize debug tests
3. **Add More Utilities**: Extend `testutil` with more common patterns
4. **Improve Documentation**: Add more examples and best practices
5. **Performance Tests**: Add dedicated performance test category
6. **Schema Tests**: Add comprehensive schema testing utilities

## 🤝 **Contributing**

When adding new tests:

1. **Choose the Right Category**: Unit, Integration, or E2E
2. **Use Common Utilities**: Leverage `testutil` packages
3. **Follow Naming Conventions**: Descriptive and consistent names
4. **Add Documentation**: Update this README for new patterns
5. **Clean Up**: Remove any debug or temporary code

This refactored structure provides a solid foundation for maintainable, scalable Kafka integration testing.
