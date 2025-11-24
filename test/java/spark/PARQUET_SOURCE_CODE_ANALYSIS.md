# Parquet Source Code Analysis: Root Cause Confirmed

## Source Code Investigation

### 1. The EOF Exception Source (`H2SeekableInputStream.java:112`)

```java
public static void readFully(Reader reader, ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
        int readCount = reader.read(buf);
        if (readCount == -1) {
            // this is probably a bug in the ParquetReader
            throw new EOFException("Reached the end of stream. Still have: " + buf.remaining() + " bytes left");
        }
    }
}
```

Comment at line 110-111: *"this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer that has more remaining than the amount of data in the stream."*

**Parquet's own code says this is a bug in Parquet!**

### 2. How Parquet Records Offsets (`ParquetFileWriter.java`)

**When writing a data page:**

```java
// Line 1027
long beforeHeader = out.getPos();  // ← GET POSITION BEFORE WRITING

// Line 1029  
if (currentChunkFirstDataPage < 0) {
    currentChunkFirstDataPage = beforeHeader;  // ← STORE THIS POSITION
}

// Then writes page header and data...
```

**When ending a column:**

```java
// Line 1593
currentOffsetIndexes.add(offsetIndexBuilder.build(currentChunkFirstDataPage));
```

**The stored offset (`currentChunkFirstDataPage`) is used in the footer!**

### 3. What Happens After Last getPos() (`ParquetFileWriter.java:2113-2119`)

```java
long footerIndex = out.getPos();
org.apache.parquet.format.FileMetaData parquetMetadata = metadataConverter.toParquetMetadata(...);
writeFileMetaData(parquetMetadata, out);  // Writes footer metadata
BytesUtils.writeIntLittleEndian(out, toIntWithCheck(out.getPos() - footerIndex, "footer")); // 4 bytes
out.write(MAGIC);  // "PAR1" - 4 bytes
```

**The last 8 bytes are:**
- 4 bytes: footer length (int32, little endian)
- 4 bytes: magic "PAR1"

This matches our logs EXACTLY!

### 4. The Complete Write Sequence

```
1. Write page data (1252 bytes)
   - Before each page: out.getPos() → records offset
   
2. End column:
   - Builds offset index using recorded offsets
   
3. End block:
   - Finalizes block metadata
   
4. End file:
   - Writes column indexes
   - Writes offset indexes  
   - Writes bloom filters
   - Writes footer metadata
   - Writes footer length (4 bytes)  ← NO GETPOS() CALL BEFORE THIS!
   - Writes MAGIC bytes (4 bytes)    ← NO GETPOS() CALL BEFORE THIS!
   
5. Close:
   - Flushes stream
```

## The Real Problem

### Scenario with Buffering:

```
Time    Action                      Virtual   Flushed   Buffer   What getPos() returns
                                    Position  Position  Content
--------------------------------------------------------------------------------
T0      Write 1252 bytes data       1252      0         1252     Returns 1252 (virtual)
T1      Parquet calls getPos()      1252      0         1252     → Records "page at 1252"
T2      Write 4 bytes (footer len)  1256      0         1256     (no getPos() call)
T3      Write 4 bytes (MAGIC)       1260      0         1260     (no getPos() call)
T4      close() → flush all         1260      1260      0        -
T5      Footer written with: "page at offset 1252"
```

### When Reading:

```
1. Read footer from end of file
2. Footer says: "page data starts at offset 1252"
3. Seek to position 1252 in the file
4. At position 1252: finds the 4-byte footer length + 4-byte MAGIC (8 bytes total!)
5. Tries to parse these 8 bytes as page header
6. Fails → "Still have: 78 bytes left"
```

## Why Our Fixes Didn't Work

### Fix 1: Virtual Position Tracking
- **What we did**: `getPos()` returns `position + buffer.position()`
- **Why it failed**: Parquet records the RETURN VALUE (1252), then writes 8 more bytes. The footer says "1252" but those 8 bytes shift everything!

### Fix 2: Flush-on-getPos()
- **What we did**: Flush buffer before returning position
- **Why it failed**: After flushing at T1, buffer is empty. Then at T2-T3, 8 bytes are written to buffer. These 8 bytes are flushed at T4, AFTER Parquet has already recorded offset 1252.

### Fix 3: Disable Buffering (bufferSize=1)
- **What we did**: Set bufferSize=1 to force immediate flush
- **Why it failed**: SAME ISSUE! Even with immediate flush, the 8 bytes at T2-T3 are written AFTER the last getPos() call.

## The REAL Issue

**Parquet's assumption**: Between calling `getPos()` and writing the footer, NO additional data will be written that affects offsets.

**Reality with our implementation**: The footer length and MAGIC bytes are written BETWEEN the last `getPos()` call and when the footer metadata (containing those offsets) is written.

## The ACTUAL Fix

We need to ensure that when Parquet writes the footer containing the offsets, those offsets point to the ACTUAL byte positions in the final file, accounting for ALL writes including the 8 footer bytes.

### Option A: Adjust offsets in footer before writing
Before writing the footer, scan all recorded offsets and adjust them by +8 (or whatever the accumulated drift is).

**Problem**: We don't control Parquet's code!

### Option B: Intercept footer writes and track drift
Impossible without modifying Parquet.

### Option C: **CORRECT SOLUTION** - Make getPos() return the FUTURE position

When `getPos()` is called, we need to return the position where the NEXT byte will be written in the FINAL file, accounting for any pending buffered data.

But we ALREADY tried this with virtualPosition!

Wait... let me re-examine our virtualPosition implementation. Maybe there's a subtle bug.

Actually, I think the issue is different. Let me reconsider...

When using virtualPosition with buffering:
- T0: Write 1252 bytes → buffer has 1252 bytes
- T1: getPos() returns virtualPosition = 1252 ✓
- Parquet records "page at 1252" ✓
- T2-T3: Write 8 bytes → buffer has 1260 bytes
- T4: Flush → writes all 1260 bytes starting at file position 0
- Result: Page data is at file position 0-1251, footer stuff is at 1252-1259

So when reading, seeking to 1252 actually finds the footer length+MAGIC, not the page data!

**THE REAL BUG**: With buffering, ALL data goes to position 0 in the file when flushed. The virtualPosition tracking is meaningless because the actual FILE positions are different from the virtual positions!

## THE SOLUTION

**We MUST flush the buffer BEFORE every getPos() call** so that:
1. When Parquet calls getPos(), the buffer is empty
2. The returned position is the actual file position
3. Subsequent writes go to the correct file positions

We tried this, but maybe our implementation had a bug. Let me check...

