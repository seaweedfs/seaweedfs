# NOOP Records Analysis

## Key Discovery: NOOP Record Structure

**User Insight**: "the actual data for NOOP is in the key field"

This explains the data distribution we've been seeing!

### Schema Registry Record Types:

1. **NOOP Records** (offsets 0, 1):
   - Key: `{"keytype":"NOOP","magic":0}` (28 bytes)
   - Value: Empty (0 bytes)
   - Purpose: Sentinel/initialization records

2. **Schema Records** (offsets 2+):
   - Key: `{"subject":"...","version":1,"id":N}` (75-77 bytes)
   - Value: Avro schema JSON (111-511 bytes)
   - Purpose: Actual schema data

### Current Behavior:

From logs we saw:
- Gateway repeatedly fetches offsets 0, 1, 2, 3... ✓ (correct progression)
- Broker sends correct data:
  - offset=0: keyLen=28, valueLen=0 (NOOP)
  - offset=1: keyLen=28, valueLen=0 (NOOP)  
  - offset=2: keyLen=77, valueLen=511 (SCHEMA!)
  - offset=3: keyLen=75, valueLen=111 (SCHEMA!)

- Gateway receives:
  - ALL offsets: keyLen=28, valueLen=0 (all NOOPs!) ✗ (DATA LOSS!)

### The Real Problem:

**Data is being lost between broker's Send() and gateway's Recv()**

The broker logs show it's sending 511-byte values, but the gateway only receives empty values. This is a gRPC stream transmission issue, NOT a record structure issue.

### Possible Causes:

1. **Memory reuse/clearing**: `logEntry.Data` gets cleared after logging but before Send()
2. **Protobuf serialization bug**: DataMessage.Value not being serialized correctly
3. **Buffer ownership**: The Data slice is being reused/recycled
4. **Chunking issue**: Large values (511 bytes) not fitting in message frame

### Next Steps:

1. ✓ Added pre-send logging to verify data exists right before Send()
2. Check if logEntry.Data is being reused/recycled
3. Verify protobuf DataMessage field definitions
4. Check if there's a maximum message size being enforced
5. Consider copying data instead of referencing: `Value: append([]byte(nil), logEntry.Data...)`

### Test Plan:

Wait for services to initialize, then check:
- Pre-send logs: Does dataMsg.Value have 511 bytes before Send()?
- Gateway logs: Does it still receive 0 bytes after Recv()?
- If yes → Data is being lost in gRPC layer
- If no → Data was already empty before Send()
