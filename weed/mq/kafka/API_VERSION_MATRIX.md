# Kafka API Version Matrix Audit

## Summary
This document audits the advertised API versions in `handleApiVersions()` against actual implementation support and identifies mismatches that need correction.

## Current Status: MISMATCHES FOUND

### API Version Discrepancies

| API Key | API Name | Advertised | Actually Implemented | Status | Action Needed |
|---------|----------|------------|---------------------|--------|---------------|
| 18 | ApiVersions | v0-v3 | v0-v3 | Match | None |
| 3 | Metadata | v0-v7 | v0-v7 | Match | None |
| 0 | Produce | v0-v7 | v0-v7 | Match | None |
| 1 | Fetch | v0-v7 | v0-v7 | Match | None |
| 2 | ListOffsets | v0-v2 | v0-v2 | Match | None |
| 19 | CreateTopics | v0-v5 | v0-v5 | Match | None |
| 20 | DeleteTopics | v0-v4 | v0-v4 | Match | None |
| 11 | JoinGroup | v0-v7 | v0-v7 | Match | None |
| 14 | SyncGroup | v0-v5 | v0-v5 | Match | None |
| 8 | OffsetCommit | v0-v2 | v0-v2 | Match | None |
| 9 | OffsetFetch | v0-v5 | v0-v5 | Match | None |
| 10 | FindCoordinator | v0-v2 | v0-v2 | Match | None |
| 12 | Heartbeat | v0-v4 | v0-v4 | Match | None |
| 13 | LeaveGroup | v0-v4 | v0-v4 | Match | None |

## Detailed Analysis

### 1. OffsetFetch API (Key 9)
- Advertised and implemented aligned at v0–v5.

### 2. CreateTopics API (Key 19)
- Advertised and implemented aligned at v0–v5.

### Validation vs Advertisement Consistency
`validateAPIVersion()` and `handleApiVersions()` are consistent with the supported ranges.

## Implementation Details

### OffsetFetch Version Features:
- **v0-v2**: Basic offset fetch
- **v3+**: Includes throttle_time_ms
- **v5+**: Includes leader_epoch for each partition

### CreateTopics Version Features:  
- **v0-v1**: Regular array format
- **v2-v5**: Compact array format, tagged fields

## Recommendations

1. Re-verify after protocol changes to keep the matrix accurate
2. Extend coverage as implementations grow; keep tests for version guards

## Test Verification

1. Exercise OffsetFetch v3–v5 with kafka-go and Sarama
2. Exercise CreateTopics v5; verify fields and tagged fields
3. Verify throttle_time_ms and leader_epoch population
4. Ensure version validation permits supported versions only

## Conservative Alternative

If we want to be conservative and not advertise versions we haven't thoroughly tested:
- **OffsetFetch**: Limit to v3 (throttle time support) instead of v5
- **CreateTopics**: Keep at v4 unless v5 is specifically needed

This would still fix the main discrepancy while being more cautious about untested version features.
