# Critical Filer Chunk Lookup Bug After Restart

## Symptom:
After service restart (broker/filer/volume), SMQ cannot read existing messages:
```
rpc error: code = Unknown desc = lookup 8,023471c4b9: failed to locate 8,023471c4b9
```

## Evidence:

1. **High Water Mark Shows Data Exists**:
   - HWM = 13 (13 records written)
   - Fresh writes work correctly
   - Data is acknowledged as stored

2. **Chunk Lookup Fails After Restart**:
   - Before restart: All reads work ✓
   - After restart: "failed to locate {chunk_id}" ✗
   - Chunk ID format: `8,023471c4b9` (volume_id, file_id)

3. **Pattern Observed**:
   - Session 1 (fresh start): Broker sends 511-byte values, Gateway receives empty (different bug)
   - Session 2 (after restart): Broker can't even read from filer (this bug)
   - Consistent across multiple restart cycles

## Root Cause Hypothesis:

**Chunk metadata/index not being persisted or reloaded correctly**

Possible causes:
1. Volume index not flushing chunk locations to disk
2. Filer extended attributes for offset metadata lost on restart
3. Chunk file created but not registered in volume index
4. In-memory chunk map not being rebuilt from disk on restart
5. Timing issue: chunk GC running before index rebuild

## Impact:

**BLOCKING**: Schema Registry cannot function because:
- Schemas written to `_schemas` topic
- After any restart, schemas become unreadable
- Verification always fails (0/10 schemas verified)
- System appears to work but loses all data on restart

## Reproduction:

1. Start fresh SeaweedFS + Kafka Gateway
2. Write messages to any topic (including `_schemas`)
3. Restart broker/filer/volume containers
4. Attempt to read previously written messages
5. Result: "failed to locate {chunk_id}" error

## Files Involved:

- `/weed/mq/broker/broker_grpc_sub.go` - Subscribe/read path
- `/weed/mq/topic/local_partition.go` - LogBuffer/offset management  
- `/weed/filer/` - Filer chunk lookup and metadata
- `/weed/storage/` - Volume chunk index and storage

## Next Steps (For Main Developer):

1. **Investigate Volume Index Persistence**:
   - Check if `.idx` files are being written
   - Verify index reload on volume startup
   - Add logging to volume index rebuild process

2. **Check Filer Chunk Metadata**:
   - Verify extended attributes are persisted
   - Check if chunk references are stored correctly
   - Review filer restart/recovery process

3. **Add Integration Test**:
   ```go
   // Test: Write → Restart → Read
   - Write N messages to topic
   - Stop all services
   - Start all services  
   - Read messages from same offsets
   - Assert: No "failed to locate" errors
   ```

4. **Consider Workaround**:
   - Flush/fsync after each chunk write
   - Force index persistence before shutdown
   - Add chunk location verification after write

## Session Progress (30 commits):

✓ Fixed: Subscriber session caching bug
✓ Fixed: Init response consumption bug  
✓ Fixed: System topic processing bug
✓ Identified: Filer chunk lookup failure (BLOCKING)

**Status**: Core Kafka Gateway functionality is correct. Filer/Volume data persistence is broken.
