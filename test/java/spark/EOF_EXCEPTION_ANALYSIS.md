# EOFException Analysis: "Still have: 78 bytes left"

## Problem Summary

Spark Parquet writes succeed, but subsequent reads fail with:
```
java.io.EOFException: Reached the end of stream. Still have: 78 bytes left
```

## What the Logs Tell Us

### Write Phase ✅ (Everything looks correct)

**year=2020 file:**
```
🔧 Created stream: position=0 bufferSize=1048576
🔒 close START: position=0 buffer.position()=696 totalBytesWritten=696
→ Submitted 696 bytes, new position=696
✅ close END: finalPosition=696 totalBytesWritten=696
Calculated file size: 696 (chunks: 696, attr: 696, #chunks: 1)
```

**year=2021 file:**
```
🔧 Created stream: position=0 bufferSize=1048576
🔒 close START: position=0 buffer.position()=684 totalBytesWritten=684
→ Submitted 684 bytes, new position=684
✅ close END: finalPosition=684 totalBytesWritten=684
Calculated file size: 684 (chunks: 684, attr: 684, #chunks: 1)
```

**Key observations:**
- ✅ `totalBytesWritten == position == buffer == chunks == attr`
- ✅ All bytes received through `write()` are flushed and stored
- ✅ File metadata is consistent
- ✅ No bytes lost in SeaweedFS layer

### Read Phase ❌ (Parquet expects more bytes)

**Consistent pattern:**
- year=2020: wrote 696 bytes, **expects 774 bytes** → missing 78
- year=2021: wrote 684 bytes, **expects 762 bytes** → missing 78

The **78-byte discrepancy is constant across both files**, suggesting it's not random data loss.

## Hypotheses

### H1: Parquet Footer Not Fully Written
Parquet file structure:
```
[Magic "PAR1" 4B] [Data pages] [Footer] [Footer length 4B] [Magic "PAR1" 4B]
```

**Possible scenario:**
1. Parquet writes 684 bytes of data pages
2. Parquet **intends** to write 78 bytes of footer metadata
3. Our `SeaweedOutputStream.close()` is called
4. Only data pages (684 bytes) make it to the file
5. Footer (78 bytes) is lost or never written

**Evidence for:**
- 78 bytes is a reasonable size for a Parquet footer with minimal metadata
- Files say "snappy.parquet" → compressed, so footer would be small
- Consistent 78-byte loss across files

**Evidence against:**
- Our `close()` logs show all bytes received via `write()` were processed
- If Parquet wrote footer to stream, we'd see `totalBytesWritten=762`

### H2: FSDataOutputStream Position Tracking Mismatch
Hadoop wraps our stream:
```java
new FSDataOutputStream(seaweedOutputStream, statistics)
```

**Possible scenario:**
1. Parquet writes 684 bytes → `FSDataOutputStream` increments position to 684
2. Parquet writes 78-byte footer → `FSDataOutputStream` increments position to 762
3. **BUT** only 684 bytes reach our `SeaweedOutputStream.write()`
4. Parquet queries `FSDataOutputStream.getPos()` → returns 762
5. Parquet writes "file size: 762" in its footer
6. Actual file only has 684 bytes

**Evidence for:**
- Would explain why our logs show 684 but Parquet expects 762
- FSDataOutputStream might have its own buffering

**Evidence against:**
- FSDataOutputStream is well-tested Hadoop core component
- Unlikely to lose bytes

### H3: Race Condition During File Rename
Files are written to `_temporary/` then renamed to final location.

**Possible scenario:**
1. Write completes successfully (684 bytes)
2. `close()` flushes and updates metadata
3. File is renamed while metadata is propagating
4. Read happens before metadata sync completes
5. Reader gets stale file size or incomplete footer

**Evidence for:**
- Distributed systems often have eventual consistency issues
- Rename might not sync metadata immediately

**Evidence against:**
- We added `fs.seaweed.write.flush.sync=true` to force sync
- Error is consistent, not intermittent

### H4: Compression-Related Size Confusion
Files use Snappy compression (`*.snappy.parquet`).

**Possible scenario:**
1. Parquet tracks uncompressed size internally
2. Writes compressed data to stream
3. Size mismatch between compressed file and uncompressed metadata

**Evidence against:**
- Parquet handles compression internally and consistently
- Would affect all Parquet users, not just SeaweedFS

## Next Debugging Steps

### Added: getPos() Logging
```java
public synchronized long getPos() {
    long currentPos = position + buffer.position();
    LOG.info("[DEBUG-2024] 📍 getPos() called: flushedPosition={} bufferPosition={} returning={}", 
            position, buffer.position(), currentPos);
    return currentPos;
}
```

**Will reveal:**
- If/when Parquet queries position
- What value is returned vs what was actually written
- If FSDataOutputStream bypasses our position tracking

### Next Steps if getPos() is NOT called:
→ Parquet is not using position tracking
→ Focus on footer write completion

### Next Steps if getPos() returns 762 but we only wrote 684:
→ FSDataOutputStream has buffering issue or byte loss
→ Need to investigate Hadoop wrapper behavior

### Next Steps if getPos() returns 684 (correct):
→ Issue is in footer metadata or read path
→ Need to examine Parquet footer contents

## Parquet File Format Context

Typical small Parquet file (~700 bytes):
```
Offset   Content
0-3      Magic "PAR1"
4-650    Row group data (compressed)
651-728  Footer metadata (schema, row group pointers)
729-732  Footer length (4 bytes, value: 78)
733-736  Magic "PAR1"
Total: 737 bytes
```

If footer length field says "78" but only data exists:
- File ends at byte 650
- Footer starts at byte 651 (but doesn't exist)
- Reader tries to read 78 bytes, gets EOFException

This matches our error pattern perfectly.

## Recommended Fix Directions

1. **Ensure footer is fully written before close returns**
2. **Add explicit fsync/hsync before metadata write**
3. **Verify FSDataOutputStream doesn't buffer separately**
4. **Check if Parquet needs special OutputStreamAdapter**

