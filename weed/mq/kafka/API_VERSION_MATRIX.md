# Kafka API Version Matrix Audit

## Summary
This document audits the advertised API versions in `handleApiVersions()` against actual implementation support in `validateAPIVersion()` and handlers.

## Current Status: ALL VERIFIED ✅

### API Version Matrix

| API Key | API Name | Advertised | Validated | Handler Implemented | Status |
|---------|----------|------------|-----------|---------------------|--------|
| 18 | ApiVersions | v0-v4 | v0-v4 | v0-v4 | ✅ Match |
| 3 | Metadata | v0-v7 | v0-v7 | v0-v7 | ✅ Match |
| 0 | Produce | v0-v7 | v0-v7 | v0-v7 | ✅ Match |
| 1 | Fetch | v0-v7 | v0-v7 | v0-v7 | ✅ Match |
| 2 | ListOffsets | v0-v2 | v0-v2 | v0-v2 | ✅ Match |
| 19 | CreateTopics | v0-v5 | v0-v5 | v0-v5 | ✅ Match |
| 20 | DeleteTopics | v0-v4 | v0-v4 | v0-v4 | ✅ Match |
| 10 | FindCoordinator | v0-v3 | v0-v3 | v0-v3 | ✅ Match |
| 11 | JoinGroup | v0-v6 | v0-v6 | v0-v6 | ✅ Match |
| 14 | SyncGroup | v0-v5 | v0-v5 | v0-v5 | ✅ Match |
| 8 | OffsetCommit | v0-v2 | v0-v2 | v0-v2 | ✅ Match |
| 9 | OffsetFetch | v0-v5 | v0-v5 | v0-v5 | ✅ Match |
| 12 | Heartbeat | v0-v4 | v0-v4 | v0-v4 | ✅ Match |
| 13 | LeaveGroup | v0-v4 | v0-v4 | v0-v4 | ✅ Match |
| 15 | DescribeGroups | v0-v5 | v0-v5 | v0-v5 | ✅ Match |
| 16 | ListGroups | v0-v4 | v0-v4 | v0-v4 | ✅ Match |
| 32 | DescribeConfigs | v0-v4 | v0-v4 | v0-v4 | ✅ Match |
| 22 | InitProducerId | v0-v4 | v0-v4 | v0-v4 | ✅ Match |
| 60 | DescribeCluster | v0-v1 | v0-v1 | v0-v1 | ✅ Match |

## Implementation Details

### Core APIs
- **ApiVersions (v0-v4)**: Supports both flexible (v3+) and non-flexible formats. v4 added for Kafka 8.0.0 compatibility.
- **Metadata (v0-v7)**: Full version support with flexible format in v7+
- **Produce (v0-v7)**: Supports transactional writes and idempotent producers
- **Fetch (v0-v7)**: Includes schema-aware fetching and multi-batch support

### Consumer Group Coordination
- **FindCoordinator (v0-v3)**: v3+ supports flexible format
- **JoinGroup (v0-v6)**: Capped at v6 (first flexible version)
- **SyncGroup (v0-v5)**: Full consumer group protocol support
- **Heartbeat (v0-v4)**: Consumer group session management
- **LeaveGroup (v0-v4)**: Clean consumer group exit
- **OffsetCommit (v0-v2)**: Consumer offset persistence
- **OffsetFetch (v0-v5)**: v3+ includes throttle_time_ms, v5+ includes leader_epoch

### Topic Management
- **CreateTopics (v0-v5)**: v2+ uses compact arrays and tagged fields
- **DeleteTopics (v0-v4)**: Full topic deletion support
- **ListOffsets (v0-v2)**: Offset listing for partitions

### Admin & Discovery
- **DescribeCluster (v0-v1)**: AdminClient compatibility (KIP-919)
- **DescribeGroups (v0-v5)**: Consumer group introspection
- **ListGroups (v0-v4)**: List all consumer groups
- **DescribeConfigs (v0-v4)**: Configuration inspection
- **InitProducerId (v0-v4)**: Transactional producer initialization

## Verification Source

All version ranges verified from `handler.go`:
- `SupportedApiKeys` array (line 1196): Advertised versions
- `validateAPIVersion()` function (line 2903): Validation ranges
- Individual handler implementations: Actual version support

Last verified: 2025-10-13

## Maintenance Notes

1. After adding new API handlers, update all three locations:
   - `SupportedApiKeys` array
   - `validateAPIVersion()` map
   - This documentation
2. Test new versions with kafka-go and Sarama clients
3. Ensure flexible format support for v3+ APIs where applicable
