# Schema Registry Debug Findings

## Summary
The Kafka Gateway's fetch protocol is working correctly - consumer offsets advance properly from 0 to 12. However, Schema Registry's internal cache is not populating, causing schema verification to fail.

## Evidence from Debug Logs

### 1. Kafka Gateway Fetch Requests (CORRECT)
```
REQUEST: offset=0 HWM=0 → (no data yet, returns empty)
REQUEST: offset=0 HWM=1 → RESPONSE: 1 batch, 96 bytes, nextOffset=1 ✓
REQUEST: offset=1 HWM=1 → (caught up, returns empty)
REQUEST: offset=1 HWM=2 → RESPONSE: 1 batch, bytes, nextOffset=2 ✓
...
REQUEST: offset=12 HWM=12 → (caught up, returns empty)
```

**Pattern**: Clean progression 0→1→2→...→12. No resets. Each fetch at offset N returns records starting at N.

### 2. Schema Registry Internal Offset (INCORRECT)
```
Reached offset at 0
Reached offset at 0
Reached offset at 1
Reached offset at 0  ← RESET!
Reached offset at 0  ← RESET!
Reached offset at 2
Reached offset at 0  ← RESET!
...
```

**Pattern**: Repeatedly resets to 0 after each schema registration.

## Root Cause Hypothesis

The pattern `0→0→1→0→0→2→0→0→3` suggests Schema Registry is seeing **duplicate records** or **incorrect base offsets in record batches**.

### Kafka Record Batch Format
Each Kafka record batch has:
- **Base Offset**: The offset of the first record in the batch
- **Last Offset Delta**: Number of records - 1
- **Records**: Individual records with offset deltas from base

**Critical Issue**: If the base offset in our record batch is always 0, then:
- Batch 1: base_offset=0, contains record at offset 0 ✓
- Batch 2: base_offset=0 (WRONG!), should be base_offset=1
  - Schema Registry sees this as another record at offset 0!
- Batch 3: base_offset=0 (WRONG!), should be base_offset=2

## Next Steps

1. **Verify Record Batch Base Offset**
   - Add logging to show what base offset we're setting in constructed record batches
   - Check `constructRecordBatchFromSMQ` and `MultiBatchFetcher` implementations

2. **Check Multi-Batch Fetcher Logic**
   - The logs show "RESPONSE: nextOffset=1" which is correct
   - But need to verify the actual bytes in the record batch have correct base_offset field

3. **Inspect Schema Registry's Record Parsing**
   - Verify SR is correctly parsing the base offset from the batch
   - Or if our record batch encoding is malformed

## Test Case
When fetching at offset 1 (after first schema is registered):
- Expected: Record batch with base_offset=1, containing 1 record
- Suspected Bug: Record batch with base_offset=0, which SR interprets as offset 0 again

## Files to Investigate
1. `weed/mq/kafka/protocol/fetch.go` - `constructRecordBatchFromSMQ`
2. `weed/mq/kafka/protocol/fetch_multi_batch.go` - `FetchMultipleBatches`
3. Kafka record batch encoding logic - base offset field encoding
