# BUG FOUND: Subscriber Session Caching Causes Stale Reads

## Root Cause Identified

The `GetOrCreateSubscriber` function caches subscriber sessions per topic-partition. When Schema Registry repeatedly requests the same offset (e.g., offset 1), it gets back the SAME session which has already consumed that record and advanced past it.

## Evidence

### 1. Kafka Gateway Requests (Correct)
```
requestOffset=0 → Returns record at offset 0 (NOOP)
requestOffset=1 → Returns record at offset 1 (NOOP)
requestOffset=1 → Returns EMPTY (session already past offset 1!)  
requestOffset=1 → Returns EMPTY (session already past offset 1!)
...never requests offset 2, 3, 4...
```

### 2. Broker Subscribe (Has Data)
```
offset=0 valueLen=0  (NOOP)
offset=1 valueLen=0  (NOOP)
offset=2 valueLen=511 (SCHEMA DATA!) ← Session reads this
offset=3 valueLen=111 (SCHEMA DATA!) ← Session reads this
```

### 3. The Caching Bug (seaweedmq_handler.go:1649-1666)
```go
func (bc *BrokerClient) GetOrCreateSubscriber(topic string, partition int32, startOffset int64) {
    if session, exists := bc.subscribers[key]; exists {
        // BUG: Only recreates if startOffset CHANGES
        if session.StartOffset != startOffset {
            // recreate session
        } else {
            return session, nil  // ← Returns STALE session!
        }
    }
}
```

## Why This Happens

1. First fetch at offset 1: Creates new session, reads record at offset 1
2. Session internally advances to offset 2
3. Second fetch at offset 1: Returns SAME session (startOffset==1)
4. Session tries to read from its current position (offset 2+), not from offset 1
5. Returns empty or wrong data

## Impact

- Schema Registry reads offset 0 (NOOP) ✓
- Schema Registry reads offset 1 (NOOP) ✓  
- Schema Registry tries to read offset 1 again (gets EMPTY) ✗
- Schema Registry NEVER sees offsets 2-11 (the actual schemas) ✗
- Cache remains empty ✗

## The Fix

**Option 1**: Don't cache subscriber sessions - create a new one for each fetch
**Option 2**: Track session's current offset and recreate if it's ahead of requested offset
**Option 3**: Use a stateless subscribe API that accepts start offset each time

I recommend **Option 1** for simplicity and correctness. Subscriber sessions should not be reused across different fetch requests.
