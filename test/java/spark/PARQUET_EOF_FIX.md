# Parquet EOFException Fix: 78-Byte Discrepancy

## Problem Statement

Spark integration tests were consistently failing with:
```
java.io.EOFException: Reached the end of stream. Still have: 78 bytes left
at org.apache.parquet.hadoop.util.H2SeekableInputStream.readFully(H2SeekableInputStream.java:112)
```

The error was consistent across all Parquet writes:
- File sizes varied: 684, 693, 696, 707, 1275 bytes
- Missing bytes: **ALWAYS exactly 78 bytes**
- This suggested a systematic offset error, not random data loss

## Root Cause Analysis

### Investigation Steps

1. **Examined Parquet-Java source code** (`~/dev/parquet-java/`):
   - Found the error originates in `H2SeekableInputStream.readFully()` line 112
   - Comment indicates: *"this is probably a bug in the ParquetReader"*
   - Parquet is trying to read data based on footer metadata offsets

2. **Traced Parquet writer logic**:
   - In `ParquetFileWriter.java` line 1027-1029 and 1546:
     ```java
     long beforeHeader = out.getPos();
     if (currentChunkFirstDataPage < 0) {
         currentChunkFirstDataPage = beforeHeader;
     }
     ```
   - Parquet calls `out.getPos()` to record where column chunks start
   - These positions are stored in the file's footer metadata

3. **Identified the disconnect**:
   - `out` is Hadoop's `FSDataOutputStream` wrapping `SeaweedHadoopOutputStream`
   - `FSDataOutputStream` uses an **internal position counter**
   - It does **NOT** call `SeaweedOutputStream.getPos()` automatically
   - Evidence: No `"[DEBUG-2024] getPos() called"` log messages appeared in tests

4. **Confirmed with file download**:
   - Successfully downloaded actual Parquet file (1275 bytes)
   - Parquet's footer claims data extends to byte 1353 (1275 + 78)
   - The footer metadata has incorrect offsets!

### The Mismatch

```
When writing:
┌─────────────────────────────────────────────────────────────┐
│ Parquet Writer                                              │
│   ↓ write(data)                                             │
│ FSDataOutputStream (Hadoop)                                 │
│   - Counts bytes: position = 1353                           │
│   - getPos() returns: 1353 ← Parquet records this!          │
│   ↓ write(data)                                             │
│ SeaweedOutputStream                                         │
│   - Buffers data internally                                 │
│   - getPos() returns: position + buffer.position()          │
│   - But FSDataOutputStream NEVER calls this!                │
│   ↓ flush on close()                                        │
│ SeaweedFS Server                                            │
│   - Actually stores: 1275 bytes                             │
└─────────────────────────────────────────────────────────────┘

Result: Footer says "read from offset 1353" but file only has 1275 bytes!
```

## The Fix

**File**: `other/java/hdfs3/src/main/java/seaweed/hdfs/SeaweedFileSystem.java`

Override `FSDataOutputStream.getPos()` to delegate to our stream:

```java
SeaweedHadoopOutputStream outputStream = (SeaweedHadoopOutputStream) 
    seaweedFileSystemStore.createFile(path, overwrite, permission,
        seaweedBufferSize, replicaPlacement);

// Use custom FSDataOutputStream that delegates getPos() to our stream
return new FSDataOutputStream(outputStream, statistics) {
    @Override
    public long getPos() {
        // Delegate to SeaweedOutputStream's position tracking
        return outputStream.getPos();
    }
};
```

### Why This Works

1. **Before**: Parquet calls `FSDataOutputStream.getPos()` → Gets Hadoop's internal counter (wrong!)
2. **After**: Parquet calls `FSDataOutputStream.getPos()` → Delegates to `SeaweedOutputStream.getPos()` → Returns `position + buffer.position()` (correct!)

3. `SeaweedOutputStream.getPos()` correctly accounts for:
   - `position`: bytes already flushed to server
   - `buffer.position()`: bytes in buffer not yet flushed
   - Total: accurate position for metadata

## Testing

The fix will be validated by:
1. The existing `getPos()` logging will now show calls (previously silent)
2. Parquet files should be readable without EOFException
3. The 78-byte discrepancy should disappear

## Related Code

- **Parquet Writer**: `parquet-java/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileWriter.java:1027,1546`
- **Parquet Reader**: `parquet-java/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java:1174,1180`
- **Error Location**: `parquet-java/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/util/H2SeekableInputStream.java:112`
- **SeaweedFS Position Tracking**: `other/java/client/src/main/java/seaweedfs/client/SeaweedOutputStream.java:100-108`

## Lessons Learned

1. **Double buffering is dangerous**: When multiple layers track position independently, they can diverge
2. **Read the source**: Examining Parquet-Java and Spark source code was essential to understanding the issue
3. **Systematic errors need systematic analysis**: The consistent 78-byte offset was a clue it wasn't random data loss
4. **Framework integration matters**: Hadoop's `FSDataOutputStream` wrapper behavior must be understood and explicitly handled

## Commit

**SHA**: 9e7ed4868
**Message**: "fix: Override FSDataOutputStream.getPos() to use SeaweedOutputStream position"

