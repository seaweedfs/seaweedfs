# Kafka Protocol Compatibility Review & TODOs

## Overview
This document identifies areas in the current Kafka implementation that need attention for full protocol compatibility, including assumptions, simplifications, and potential issues.

## Critical Protocol Issues

### HIGH PRIORITY - Protocol Breaking Issues

#### 1. **Record Batch Parsing (Produce API)**
**Files**: `protocol/record_batch_parser.go`, `protocol/produce.go`
**Status**:
- Compression implemented (gzip, snappy, lz4, zstd)
- CRC validation available in `ParseRecordBatchWithValidation`
**Remaining**:
- Parse individual records (varints, headers, control records, timestamps)
- Integrate real record extraction into Produce path (replace simplified fallbacks)
**TODOs**:
```go
// TODO: Implement full record parsing for v2 batches
// - Varint decode for per-record fields and headers
// - Control records and transaction markers
// - Accurate timestamp handling
```

#### 2. **Request Parsing Assumptions**
**Files**: `protocol/offset_management.go`, `protocol/joingroup.go`, `protocol/consumer_coordination.go`
**Issues**:
- Most parsing functions have hardcoded topic/partition assumptions
- Missing support for array parsing (topics, partitions, group protocols)
- Simplified request structures that don't match real Kafka clients

**TODOs**:
```go
// TODO: Fix OffsetCommit/OffsetFetch request parsing
// Currently returns hardcoded "test-topic" with partition 0
// Need to parse actual topics array from request body

// TODO: Fix JoinGroup protocol parsing
// Currently ignores group protocols array and subscription metadata
// Need to extract actual subscribed topics from consumer metadata

// TODO: Add support for batch operations
// OffsetCommit can commit multiple topic-partitions
// LeaveGroup can handle multiple members leaving
```

#### 3. **Fetch Record Construction**
**File**: `protocol/fetch.go`
**Status**:
- Basic batching works; needs proper record encoding
**Remaining**:
- Build real record batches from stored records with proper varints and headers
- Honor timestamps and per-record metadata
**TODOs**:
```go
// TODO: Implement real batch construction for Fetch
// - Proper varint encode for lengths and deltas
// - Include headers and correct timestamps
// - Support v2 record batch layout end-to-end
```

### MEDIUM PRIORITY - Compatibility Issues

#### 4. **API Version Support**
**File**: `protocol/handler.go`
**Status**: Advertised ranges updated to match implemented (e.g., CreateTopics v5, OffsetFetch v5)
**Remaining**: Maintain alignment as handlers evolve; add stricter per-version validation paths

**TODOs**:
```go
// TODO: Add API version validation per request
// Different API versions have different request/response formats
// Need to validate apiVersion from request header and respond accordingly

// TODO: Update handleApiVersions to reflect actual supported features
// Current max versions may be too optimistic for partial implementations
```

#### 5. **Consumer Group Protocol Metadata**
**File**: `protocol/joingroup.go`
**Issues**:
- Consumer subscription extraction is hardcoded to return `["test-topic"]`
- Group protocol metadata parsing is completely stubbed

**TODOs**:
```go
// TODO: Implement proper consumer protocol metadata parsing
// Consumer clients send subscription information in protocol metadata
// Need to decode consumer subscription protocol format:
// - Version(2) + subscription topics + user data

// TODO: Support multiple assignment strategies properly
// Currently only basic range/roundrobin, need to parse client preferences
```

#### 6. **Error Code Mapping**
**Files**: Multiple protocol files
**Issues**:
- Some error codes may not match Kafka specifications exactly
- Missing error codes for edge cases

**TODOs**:
```go
// TODO: Verify all error codes match Kafka specification
// Check ErrorCode constants against official Kafka protocol docs
// Some custom error codes may not be recognized by clients

// TODO: Add missing error codes for:
// - Network errors, timeout errors
// - Quota exceeded, throttling
// - Security/authorization errors
```

### LOW PRIORITY - Implementation Completeness

#### 7. **Connection Management**
**File**: `protocol/handler.go`
**Issues**:
- Basic connection handling without connection pooling
- No support for SASL authentication or SSL/TLS
- Missing connection metadata (client host, version)

**TODOs**:
```go
// TODO: Extract client connection metadata
// JoinGroup requests need actual client host instead of "unknown-host"
// Parse client version from request headers for better compatibility

// TODO: Add connection security support
// Support SASL/PLAIN, SASL/SCRAM authentication
// Support SSL/TLS encryption
```

#### 8. **Record Timestamps and Offsets**
**Files**: `protocol/produce.go`, `protocol/fetch.go`
**Issues**:
- Simplified timestamp handling
**Note**: Offset assignment is handled by SMQ native offsets; ensure Kafka-visible semantics map correctly

**TODOs**:
```go
// TODO: Implement proper offset assignment strategy
// Kafka offsets are partition-specific and strictly increasing
// Current implementation may have gaps or inconsistencies

// TODO: Support timestamp types correctly
// Kafka supports CreateTime vs LogAppendTime
// Need to handle timestamp-based offset lookups properly
```

#### 9. **SeaweedMQ Integration Assumptions**
**File**: `integration/seaweedmq_handler.go`
**Issues**:
- Simplified record format conversion
- Single partition assumption for new topics
- Missing topic configuration support

**TODOs**:
```go
// TODO: Implement proper Kafka->SeaweedMQ record conversion
// Currently uses placeholder keys/values
// Need to extract actual record data from Kafka record batches

// TODO: Support configurable partition counts
// Currently hardcoded to 1 partition per topic
// Need to respect CreateTopics partition count requests

// TODO: Add topic configuration support
// Kafka topics have configs like retention, compression, cleanup policy
// Map these to SeaweedMQ topic settings
```

## Testing Compatibility Issues

### Missing Integration Tests
**TODOs**:
```go
// TODO: Add real Kafka client integration tests
// Test with kafka-go, Sarama, and other popular Go clients
// Verify producer/consumer workflows work end-to-end

// TODO: Add protocol conformance tests
// Use Kafka protocol test vectors if available
// Test edge cases and error conditions

// TODO: Add load testing
// Verify behavior under high throughput
// Test with multiple concurrent consumer groups
```

### Protocol Version Testing
**TODOs**:
```go
// TODO: Test multiple API versions
// Clients may use different API versions
// Ensure backward compatibility

// TODO: Test with different Kafka client libraries
// Java clients, Python clients, etc.
// Different clients may have different protocol expectations
```

## Performance & Scalability TODOs

### Memory Management
**TODOs**:
```go
// TODO: Add memory pooling for large messages
// Avoid allocating large byte slices for each request
// Reuse buffers for protocol encoding/decoding

// TODO: Implement streaming for large record batches
// Don't load entire batches into memory at once
// Stream records directly from storage to client
```

### Connection Handling
**TODOs**:
```go
// TODO: Add connection timeout handling
// Implement proper client timeout detection
// Clean up stale connections and consumer group members

// TODO: Add backpressure handling
// Implement flow control for high-throughput scenarios
// Prevent memory exhaustion during load spikes
```

## Immediate Action Items

### Phase 4 Priority List:
1. Full record parsing for Produce (v2) and real Fetch batch construction
2. Proper request parsing (arrays) in OffsetCommit/OffsetFetch and group APIs
3. Consumer protocol metadata parsing (Join/Sync)
4. Maintain API version alignment and validation

### Compatibility Validation:
1. **Test with kafka-go library** - Most popular Go Kafka client
2. **Test with Sarama library** - Alternative popular Go client  
3. **Test with Java Kafka clients** - Reference implementation
4. **Performance benchmarking** - Compare against Apache Kafka

## Protocol Standards References

- **Kafka Protocol Guide**: https://kafka.apache.org/protocol.html
- **Record Batch Format**: Kafka protocol v2 record format specification
- **Consumer Protocol**: Group coordination and assignment protocol details
- **API Versioning**: How different API versions affect request/response format

## Notes on Current State

### What Works Well:
- Basic produce/consume flow for simple cases
- Consumer group coordination state management  
- In-memory testing mode for development
- Graceful error handling for most common cases

### What Needs Work:
- Real-world client compatibility (requires fixing parsing issues)
- Performance under load (needs compression, streaming)
- Production deployment (needs security, monitoring)
- Edge case handling (various protocol versions, error conditions)

---

**This review should be updated as protocol implementations improve and more compatibility issues are discovered.**
