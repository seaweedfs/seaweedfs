# Bind Mount Analysis - Data Persistence Investigation

## Setup:
- Switched from Docker managed volumes to local bind mounts
- Can now inspect actual files on host filesystem
- All SeaweedFS data directories mapped to `./data/`

## Key Findings:

### 1. **No Volume Files Created**
```bash
$ find data/ -name "*.dat" -o -name "*.idx"
# No results - no volume data files exist
```

**Observation**: Even after writing 5 schemas (7 records total), no `.dat` or `.idx` files are created in the volume directory.

### 2. **Data Exists in Memory Only**
- High Water Mark: 7 (confirms 7 records written)
- Schema Registry can read during active session
- But data is lost after restart

### 3. **No Chunk Lookup Failures with Fresh Data**
- When data is in LogBuffer (memory): ✓ Reads work
- When data should be on disk but isn't: ✗ Chunk lookup fails

### 4. **Filer Metadata Persists**
```bash
$ find data/seaweedfs-filer/ -name "*.log"
data/seaweedfs-filer/filerldb2/03/000001.log
data/seaweedfs-filer/filerldb2/04/000001.log
# ... (multiple LevelDB files exist)
```

**Observation**: Filer metadata is being persisted, but volume data is not.

## Root Cause Analysis:

### **Data Flow Issue**:
1. **Write Path**: Data goes to LogBuffer (memory) ✓
2. **Persistence Path**: LogBuffer → Volume files (.dat/.idx) ✗ **BROKEN**
3. **Read Path**: 
   - From memory: Works ✓
   - From disk: Fails (chunk not found) ✗

### **The Missing Link**:
The LogBuffer data is **never being flushed to volume files**. This could be due to:

1. **Buffer Size Threshold**: LogBuffer waits for more data before flushing
2. **Time-based Flush**: No periodic flush mechanism
3. **Shutdown Hook Missing**: Data not flushed on graceful shutdown
4. **Volume Assignment Issue**: LogBuffer doesn't know which volume to write to

## Evidence:

### **Before Bind Mounts** (Docker volumes):
- Same issue occurred but couldn't inspect files
- Saw "failed to locate chunk_id" errors
- Assumed it was a chunk corruption issue

### **With Bind Mounts** (local directories):
- Can confirm NO volume files are created
- Data never leaves memory (LogBuffer)
- Chunk lookup fails because chunks were never written

## Implications:

### **For SeaweedFS MQ**:
- **Critical Bug**: Data is not being persisted to disk
- **Data Loss**: All messages lost on restart
- **Production Risk**: System appears to work but has no durability

### **For Schema Registry Integration**:
- Works during active session (memory reads)
- Fails after any restart (no disk persistence)
- Explains 0/10 schema verification failures

## Next Steps:

### **Immediate Investigation**:
1. **LogBuffer Flush Logic**: When/how does LogBuffer write to volume?
2. **Volume Assignment**: Is LogBuffer properly connected to volume server?
3. **Flush Triggers**: Size threshold, time-based, or manual flush?

### **Code Areas to Check**:
- `weed/mq/topic/local_partition.go` - LogBuffer management
- `weed/storage/volume.go` - Volume write operations  
- `weed/mq/broker/` - Broker-to-volume communication

### **Test to Add**:
```go
// Test: Write → Flush → Restart → Read
- Write messages to topic
- Force LogBuffer flush (if mechanism exists)
- Restart services
- Verify data persists and is readable
```

## Conclusion:

The bind mount investigation revealed the **true root cause**: 
- **Not** a chunk lookup bug
- **Not** a filer corruption issue  
- **IS** a data persistence bug - LogBuffer never flushes to disk

This is a **critical durability issue** that affects all SeaweedFS MQ functionality.
