# Kafka Gateway TODO (Concise)

## Produce/Fetch correctness
- Implement full v2 record parsing: varints, headers, control records, timestamps
- Replace dummy Fetch batches with real per-record construction and varint encoding
- Stream large batches; avoid loading entire sets in memory; enforce maxBytes/minBytes

## Protobuf completeness
- Resolve imports/nested types from FileDescriptorSet; support fully-qualified names
- Handle Confluent Protobuf indexes; pick correct message within descriptor
- Robust decode/encode for scalars, maps, repeated, oneof; add validation APIs

## Protocol parsing and coordination
- Remove hardcoded topic/partition assumptions across handlers
- Properly parse arrays in OffsetCommit/OffsetFetch and other APIs
- Implement consumer protocol metadata parsing (JoinGroup/SyncGroup)

## API versioning and errors
- Validate per-API version shapes; align validateAPIVersion with advertised ranges
- Audit error codes against Kafka spec; add missing codes (quota, auth, timeouts)

## Schema registry and evolution
- Subject-level compatibility settings; lineage and soft-deletion
- Optional mirroring of schemas to Filer; cache TTL controls

## Offsets and consumer groups
- Prefer SMQ native offsets end-to-end; minimize legacy timestamp translation
- ListOffsets timestamp lookups backed by SMQ; group lag/high-watermark metrics

## Security and operations
- TLS listener config; SASL/PLAIN (then SCRAM) auth
- Backpressure, memory pooling, connection cleanup, metrics (Prometheus)

## Tests and benchmarks
- E2E with kafka-go, Sarama, Java; compressed batches; long-disconnect CG recovery
- Performance/regression tests for compression + CRC across clients


