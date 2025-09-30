# CRITICAL BUG: Offset-Based Filtering Not Implemented

## Discovery Date
September 30, 2025

## Commits
- **37 commits**: Added `-logFlushInterval` CLI option
- **38 commits**: Fixed offset-based filtering in disk reader

## The Problem

Schema Registry repeatedly failed verification (0/10 schemas), always receiving offset 0 (NOOP record) instead of the requested offsets (2, 3, 4, etc.).

## Root Cause Analysis

### What We Thought Was Happening
1. Schema Registry requests offset 2 → ✅ Confirmed via logs
2. Kafka Gateway receives request → ✅ Confirmed
3. `CreateFreshSubscriber` sends `EXACT_OFFSET` with offset 2 → ✅ Confirmed
4. `getRequestPosition` creates offset-based `MessagePosition` → ✅ Confirmed  
5. Broker sends correct record → ❌ **FAILED HERE**

### The Actual Bug

The disk reader (`read_log_from_disk.go`) was **only filtering by timestamp**, never by offset!

```go
// OLD CODE (BROKEN):
if logEntry.TsNs <= starTsNs {
    pos += 4 + int(size)
    continue  // Skip this record
}
```

### Why This Failed

1. **Offset-based position encoding**:
   - `NewMessagePositionFromOffset(offset)` stores offset in `MessagePosition.BatchIndex`
   - Sets `MessagePosition.Time` to `OffsetBasedPositionSentinel`

2. **Disk reader ignored offset**:
   - Only checked `logEntry.TsNs` (timestamp)
   - Never checked `logEntry.Offset`
   - Never called `startPosition.IsOffsetBased()`

3. **Result**:
   - All records passed the timestamp filter
   - But only offset 0 was returned (likely due to in-memory buffer state)
   - Schema Registry repeatedly received NOOP at offset 0

## The Fix

### Code Changes (`weed/mq/logstore/read_log_from_disk.go`)

1. **Detect offset-based subscriptions**:
```go
isOffsetBased := startPosition.IsOffsetBased()
var startOffset int64
if isOffsetBased {
    startOffset = startPosition.BatchIndex
    glog.Infof("📍 OFFSET-BASED READ: topic=%s startOffset=%d", t.Name, startOffset)
}
```

2. **Filter by offset when appropriate**:
```go
// Filter by offset if this is an offset-based subscription
if isOffsetBased {
    if logEntry.Offset < startOffset {
        pos += 4 + int(size)
        continue  // Skip records before requested offset
    }
} else {
    // Filter by timestamp for timestamp-based subscriptions
    if logEntry.TsNs <= starTsNs {
        pos += 4 + int(size)
        continue
    }
}
```

3. **Pass offset parameters through the call chain**:
   - `eachChunkFn`: Added `startOffset` and `isOffsetBased` parameters
   - `eachFileFn`: Added `startOffset` and `isOffsetBased` parameters
   - Main return function: Extracts offset from `MessagePosition`

## Impact

### Before Fix
- Schema Registry: **0/10 schemas verified**
- SR repeatedly read offset 0 (NOOP)
- SR cache never populated with actual schemas
- All schema lookups failed with 404

### After Fix (Expected)
- Schema Registry: **10/10 schemas verified**
- SR reads correct offsets (0, 2, 3, 4, ...)
- SR cache populates correctly
- Schema lookups succeed

## Related Bugs Fixed in This Session

1. **Subscriber Session Caching** (Commit 30): `CreateFreshSubscriber` implemented
2. **Init Response Consumption** (Commit 31): Removed `stream.Recv()` from init
3. **System Topic Raw Bytes** (Commit 32): Return raw bytes for `_schemas` topic
4. **Data Persistence** (Commits 35-36): Fixed volume directory and flush interval
5. **Offset-Based Filtering** (Commit 38): **THIS FIX**

## Testing

Running `make quick-test` to verify Schema Registry verification now succeeds.

## Lesson Learned

**Offset semantics matter!**

Kafka's offset-based positioning is fundamentally different from timestamp-based positioning. When implementing Kafka protocol compatibility, ensure **both** filtering modes are supported:

- **Timestamp-based**: Filter by `logEntry.TsNs`  
- **Offset-based**: Filter by `logEntry.Offset`

The code path must detect which mode is active and apply the appropriate filter.
