# Kafka API Version Matrix Audit

## Summary
This document audits the advertised API versions in `handleApiVersions()` against actual implementation support and identifies mismatches that need correction.

## Current Status: MISMATCHES FOUND ⚠️

### API Version Discrepancies

| API Key | API Name | Advertised | Actually Implemented | Status | Action Needed |
|---------|----------|------------|---------------------|--------|---------------|
| 18 | ApiVersions | v0-v3 | v0-v3 | ✅ Match | None |
| 3 | Metadata | v0-v7 | v0-v7 | ✅ Match | None |
| 0 | Produce | v0-v7 | v0-v7 | ✅ Match | None |
| 1 | Fetch | v0-v7 | v0-v7 | ✅ Match | None |
| 2 | ListOffsets | v0-v2 | v0-v2 | ✅ Match | None |
| 19 | CreateTopics | v0-v4 | v0-v5 | ❌ Mismatch | Update advertised to v0-v5 |
| 20 | DeleteTopics | v0-v4 | v0-v4 | ✅ Match | None |
| 11 | JoinGroup | v0-v7 | v0-v7 | ✅ Match | None |
| 14 | SyncGroup | v0-v5 | v0-v5 | ✅ Match | None |
| 8 | OffsetCommit | v0-v2 | v0-v2 | ✅ Match | None |
| 9 | OffsetFetch | v0-v2 | v0-v5+ | ❌ MAJOR Mismatch | Update advertised to v0-v5 |
| 10 | FindCoordinator | v0-v2 | v0-v2 | ✅ Match | None |
| 12 | Heartbeat | v0-v4 | v0-v4 | ✅ Match | None |
| 13 | LeaveGroup | v0-v4 | v0-v4 | ✅ Match | None |

## Detailed Analysis

### 1. OffsetFetch API (Key 9) - CRITICAL MISMATCH
- **Advertised**: v0-v2 (max version 2)
- **Actually Implemented**: Up to v5+
  - **Evidence**: `buildOffsetFetchResponse()` includes `if apiVersion >= 5` for leader epoch
  - **Evidence**: `if apiVersion >= 3` for throttle time
- **Impact**: Clients may not use advanced features available in v3-v5
- **Action**: Update advertised max version from 2 to 5

### 2. CreateTopics API (Key 19) - MINOR MISMATCH  
- **Advertised**: v0-v4 (max version 4)
- **Actually Implemented**: v0-v5
  - **Evidence**: `handleCreateTopics()` routes v5 requests to `handleCreateTopicsV2Plus()`
  - **Evidence**: Tests validate v0-v5 versions
- **Impact**: v5 clients may not connect expecting v5 support
- **Action**: Update advertised max version from 4 to 5

### 3. Validation vs Advertisement Inconsistency
The `validateAPIVersion()` function matches the advertised versions, which means it will incorrectly reject valid v3-v5 OffsetFetch requests that the handler can actually process.

## Implementation Details

### OffsetFetch Version Features:
- **v0-v2**: Basic offset fetch
- **v3+**: Includes throttle_time_ms
- **v5+**: Includes leader_epoch for each partition

### CreateTopics Version Features:  
- **v0-v1**: Regular array format
- **v2-v5**: Compact array format, tagged fields

## Recommendations

1. **Immediate Fix**: Update `handleApiVersions()` to advertise correct max versions
2. **Consistency Check**: Update `validateAPIVersion()` to match the corrected advertised versions  
3. **Testing**: Verify that higher version clients can successfully connect and use advanced features
4. **Documentation**: Update any client guidance to reflect the correct supported versions

## Test Verification Needed

After fixes:
1. Test OffsetFetch v3, v4, v5 with real Kafka clients
2. Test CreateTopics v5 with real Kafka clients  
3. Verify throttle_time_ms and leader_epoch are correctly populated
4. Ensure version validation doesn't reject valid higher version requests

## Conservative Alternative

If we want to be conservative and not advertise versions we haven't thoroughly tested:
- **OffsetFetch**: Limit to v3 (throttle time support) instead of v5
- **CreateTopics**: Keep at v4 unless v5 is specifically needed

This would still fix the main discrepancy while being more cautious about untested version features.
