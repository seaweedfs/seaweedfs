# Debug Breakthrough: Root Cause Identified

## Complete Event Sequence

### 1. Write Pattern
```
- writeCalls 1-465: Writing Parquet data
- Last getPos() call: writeCalls=465, returns 1252
  → flushedPosition=0 + bufferPosition=1252 = 1252
  
- writeCalls 466-470: 5 more writes (8 bytes total)
  → These are footer metadata bytes
  → Parquet does NOT call getPos() after these writes
  
- close() called:
  → buffer.position()=1260 (1252 + 8)
  → All 1260 bytes flushed to disk
  → File size set to 1260 bytes
```

### 2. The Problem

**Parquet's write sequence:**
1. Write column chunk data, calling `getPos()` after each write → records offsets
2. **Last `getPos()` returns 1252**
3. Write footer metadata (8 bytes) → **NO getPos() call!**
4. Close file → flushes all 1260 bytes

**Result**: Parquet footer says data ends at **1252**, but file actually has **1260** bytes.

### 3. The Discrepancy

```
Last getPos(): 1252 bytes  (what Parquet recorded in footer)
Actual file:   1260 bytes  (what was flushed)
Missing:       8 bytes     (footer metadata written without getPos())
```

### 4. Why It Fails on Read

When Parquet tries to read the file:
- Footer says column chunks end at offset 1252
- Parquet tries to read from 1252, expecting more data
- But the actual data structure is offset by 8 bytes
- Results in: `EOFException: Still have: 78 bytes left`

### 5. Key Insight: The "78 bytes"

The **78 bytes** is NOT missing data — it's a **metadata mismatch**:
- Parquet footer contains incorrect offsets
- These offsets are off by 8 bytes (the final footer writes)
- When reading, Parquet calculates it needs 78 more bytes based on wrong offsets

## Root Cause

**Parquet assumes `getPos()` reflects ALL bytes written, even buffered ones.**

Our implementation is correct:
```java
public long getPos() {
    return position + buffer.position();  // ✅ Includes buffered data
}
```

BUT: Parquet writes footer metadata AFTER the last `getPos()` call, so those 8 bytes
are not accounted for in the footer's offset calculations.

## Why Unit Tests Pass but Spark Fails

**Unit tests**: Direct writes → immediate getPos() → correct offsets
**Spark/Parquet**: Complex write sequence → footer written AFTER last getPos() → stale offsets

## The Fix

We need to ensure that when Parquet writes its footer, ALL bytes (including those 8 footer bytes)
are accounted for in the file position. Options:

1. **Force flush on getPos()** - ensures position is up-to-date
2. **Override FSDataOutputStream more deeply** - intercept all write operations
3. **Investigate Parquet's footer writing logic** - understand why it doesn't call getPos()

Next: Examine how HDFS/S3 FileSystem implementations handle this.
