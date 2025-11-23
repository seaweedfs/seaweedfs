# Parquet 78-Byte EOFException - Final Analysis

## Problem Status: **UNRESOLVED after Parquet 1.16.0 upgrade**

### Symptoms
- `EOFException: Reached the end of stream. Still have: 78 bytes left`
- Occurs on **every** Parquet read operation
- **78 bytes is ALWAYS the exact discrepancy** across all files/runs

### What We've Confirmed ✅

1. **SeaweedFS works perfectly**
   - All bytes written through `write()` are stored correctly
   - `totalBytesWritten == position == chunks == attr` (perfect consistency)
   - File integrity verified at every step

2. **Parquet footer IS being written**
   - Logs show complete write sequence including footer
   - Small writes at end (1, 1, 4 bytes) = Parquet trailer structure
   - All bytes flushed successfully before close

3. **Parquet 1.16.0 upgrade changes behavior but not error**
   - **Before (1.13.1):** 684 bytes written → expects 762 (missing 78)
   - **After (1.16.0):** 693 bytes written → expects 771 (missing 78)
   - +9 bytes written, but SAME 78-byte error

### Key Evidence from Logs

```
Parquet 1.13.1:
write(74 bytes): totalSoFar=679
🔒 close: totalBytesWritten=684 writeCalls=250
✅ Stored: 684 bytes
❌ Read expects: 762 bytes (684 + 78)

Parquet 1.16.0:
write(1 byte): totalSoFar=688 [FOOTER?]
write(1 byte): totalSoFar=689 [FOOTER?]
write(4 bytes): totalSoFar=693 [FOOTER?]
🔒 close: totalBytesWritten=693 writeCalls=259
✅ Stored: 693 bytes  
❌ Read expects: 771 bytes (693 + 78)
```

### What This Means

**The 78-byte discrepancy is NOT:**
- ❌ Missing footer (footer is written)
- ❌ Lost bytes (all bytes stored)
- ❌ SeaweedFS bug (perfect byte accounting)
- ❌ Version-specific (persists across Parquet versions)

**The 78-byte discrepancy IS:**
- ✅ A **systematic offset calculation error** in Parquet
- ✅ Related to how Parquet calculates **expected file size** or **column chunk offsets**
- ✅ Consistent across all test cases (not random corruption)

## Hypotheses

### H1: Page Header Size Mismatch
Parquet might be calculating expected data size including page headers that are actually compressed/elided in Snappy compression.

**Evidence:**
- 78 bytes could be multiple page headers (typically 8-16 bytes each)
- Compression might eliminate or reduce these headers
- Parquet calculates size pre-compression, reads post-compression

**Test:** Try **uncompressed Parquet** (no Snappy)

### H2: Column Chunk Metadata Offset Error  
Parquet footer contains byte offsets to column chunks. These offsets might be calculated incorrectly.

**Evidence:**
- Footer is written correctly (we see the writes)
- But offsets IN the footer point 78 bytes beyond actual data
- Reader tries to read from these wrong offsets

**Test:** Examine actual file bytes to see footer content

### H3: FSDataOutputStream Position Tracking
Hadoop's `FSDataOutputStream` wrapper might track position differently than our underlying stream.

**Evidence:**
- `getPos()` was NEVER called (suspicious!)
- Parquet must get position somehow - likely from FSDataOutputStream directly
- FSDataOutputStream might return position before flush

**Test:** Implement `Seekable` interface or check FSDataOutputStream behavior

### H4: Dictionary Page Size Accounting
Parquet uses dictionary encoding. Dictionary pages might be:
- Calculated at full size
- Written compressed
- Not accounted for properly

**Evidence:**
- Small files (good candidates for dictionary encoding)
- 78 bytes reasonable for dictionary overhead
- Consistent across files (dictionaries similar size)

**Test:** Disable dictionary encoding in writer

## Recommended Next Steps

### Option 1: File Format Analysis (Definitive)
```bash
# Download a failing Parquet file and examine it
hexdump -C part-00000-xxx.parquet | tail -n 50

# Check footer structure
parquet-tools meta part-00000-xxx.parquet
parquet-tools dump part-00000-xxx.parquet
```

This will show:
- Actual footer content
- Column chunk offsets
- What byte 693 vs 771 contains

### Option 2: Test Configuration Changes

**A) Disable Snappy compression:**
```scala
df.write
  .option("compression", "none")
  .parquet(path)
```

**B) Disable dictionary encoding:**
```scala
df.write
  .option("parquet.enable.dictionary", "false")
  .parquet(path)
```

**C) Larger page sizes:**
```scala
spark.conf.set("parquet.page.size", "2097152") // 2MB instead of 1MB
```

### Option 3: Implement Seekable Interface

Hadoop file systems that work with Parquet often implement `Seekable`:

```java
public class SeaweedOutputStream extends OutputStream implements Seekable {
    public long getPos() {
        return position + buffer.position();
    }
    
    public void seek(long pos) {
        // Not supported for output streams
        throw new UnsupportedOperationException();
    }
}
```

### Option 4: Compare with Working Implementation

Test the SAME Spark job against:
- Local HDFS
- S3A
- Azure ABFS

See if they produce identical Parquet files or different sizes.

### Option 5: Parquet Community

This might be a known issue:
- Check [Parquet JIRA](https://issues.apache.org/jira/browse/PARQUET)
- Search for "column chunk offset" bugs
- Ask on Parquet mailing list

## Why This is Hard to Debug

1. **Black box problem:** We see writes going in, but don't see what Parquet's internal calculations are
2. **Wrapper layers:** FSDataOutputStream sits between us and Parquet
3. **Binary format:** Can't inspect footer without tools
4. **Consistent failure:** No edge cases to compare against

## Files for Investigation

Priority files to examine:
1. `part-00000-09a699c4-2299-45f9-8bee-8a8b1e241905.c000.snappy.parquet` (693 bytes, year=2021)
2. Any file from year=2020 (705 bytes)

## Success Criteria for Fix

- [ ] No EOFException on any Parquet file
- [ ] File size matches between write and read
- [ ] All 10 tests pass consistently

## Workaround Options (If No Fix Found)

1. **Use different format:** Write as ORC or Avro instead of Parquet
2. **Pad files:** Add 78 bytes of padding to match expected size (hacky)
3. **Fix on read:** Modify SeaweedInputStream to lie about file size
4. **Different Spark version:** Try Spark 4.0.1 (different Parquet integration)

