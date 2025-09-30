# Subscriber Timeout Bug - Session 52

## Status: IN PROGRESS

**Date**: September 30, 2025  
**Commits**: 52

## Verified Working

✅ **Schema Registry Format**: Gateway correctly sends JSON-serialized keys/values
- NOOP: `{"keytype":"NOOP","magic":0}` (key), empty value
- SCHEMA: `{"keytype":"SCHEMA","subject":"...",  "version":1,"magic":1}` (key), JSON value

✅ **System Topic Handling**: `decodeRecordValueToKafkaMessage` correctly returns raw bytes for `_schemas` topic

✅ **Data Storage**: Produce path stores Schema Registry messages as raw JSON (no RecordValue wrapping for system topics)

## Current Bug

### Symptom
Schema Registry fetch times out after successfully retrieving ~20-23 records:
- Offsets 0-22: ✅ Success (retrieved in ~0.1s)
- Offset 23: ❌ Timeout (30s hang, then retry)

### Evidence

```
18:48:56.102 GetStoredRecords: fromOffset=23
18:48:56.104 CreateFreshSubscriber: Sending init request for offset=23
[NO "Session created successfully" log]
[NO "Received record" log]
18:49:26.295 [30s later] Same request retried after timeout
```

### Root Cause

**`CreateFreshSubscriber` or broker not responding after ~20 fetches**

Possible causes:
1. Broker resource leak (streams not being closed?)
2. Gateway gRPC connection issue
3. Broker deadlock after multiple subscriber sessions
4. Data availability issue despite HWM=31

### Broker Behavior

- First fetch of offset 23: ✅ Works (logs show "Sending _schemas record")
- Second fetch of offset 23: ❌ No broker logs (not receiving subscribe request OR not responding)

### Schema Registry Behavior

```
[2025-09-30 18:48:55,990] INFO Wait to catch up until the offset at 29
[2025-09-30 18:49:26,172] INFO Disconnecting from node 1 due to request timeout
[2025-09-30 18:49:26,174] INFO Cancelled in-flight FETCH request (30072ms)
```

## Debugging Added

1. **fetch_multibatch.go:268**: Shows key/value strings for `_schemas` topic
   - Confirmed JSON format is correct
   - Confirmed data is being decoded properly

2. **seaweedmq_handler.go**: Logs for `CreateFreshSubscriber` and `ReadRecords`
   - Shows when sessions are created
   - Shows when records are received
   - **Reveals that some sessions never complete**

3. **broker_grpc_sub.go**: Logs for data being sent from broker
   - Shows broker successfully sends data for first ~20 fetches
   - No logs for subsequent fetches (not receiving requests?)

## Next Steps

1. **Investigate subscriber session cleanup**: Are old sessions being properly closed?
2. **Check broker gRPC stream limits**: Is there a max concurrent streams limit?
3. **Add timeout logging**: Where exactly is the 30s wait happening?
4. **Test with single fetch**: Does fetching offset 23 alone (without 0-22) work?
5. **Check broker partition lock**: Is the partition getting locked/stuck?

## Timeline

- **Offset 0-22**: Retrieved successfully in multibatch
- **Offset 23 (attempt 1)**: ✅ Success at 18:48:51
- **Offset 23 (attempt 2)**: ❌ Timeout at 18:48:56, retry at 18:49:26
- **Impact**: Schema Registry cannot initialize, all schema registrations timeout

## Session Summary

- **52 commits total**
- **Major findings**:
  1. ✅ System topic format is CORRECT
  2. ✅ JSON serialization is CORRECT
  3. ❌ Subscriber timeout after ~20 fetches (NEW BUG)
