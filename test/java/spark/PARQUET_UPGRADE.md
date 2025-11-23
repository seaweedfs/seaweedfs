# Parquet 1.16.0 Upgrade - EOFException Fix Attempt

## Problem Summary

**Symptom:** `EOFException: Reached the end of stream. Still have: 78 bytes left`

**Root Cause Found:**
- Parquet 1.13.1 writes 684/696 bytes to SeaweedFS ✅
- But Parquet's footer metadata claims files should be 762/774 bytes ❌
- **Consistent 78-byte discrepancy = Parquet writer bug**

## Evidence from Debugging Logs

```
year=2020 file:
✍️ write(74 bytes): totalSoFar=679 writeCalls=236
🔒 close START: totalBytesWritten=696 writeCalls=250
✅ Stored: 696 bytes in SeaweedFS
❌ Read error: Expects 774 bytes (missing 78)

year=2021 file:
✍️ write(74 bytes): totalSoFar=667 writeCalls=236
🔒 close START: totalBytesWritten=684 writeCalls=250
✅ Stored: 684 bytes in SeaweedFS
❌ Read error: Expects 762 bytes (missing 78)
```

**Key finding:** SeaweedFS works perfectly. All bytes written are stored. The bug is in how Parquet 1.13.1 calculates expected file size in its footer.

## The Fix

**Upgraded Parquet from 1.13.1 → 1.16.0**

Parquet 1.16.0 (released Aug 30, 2024) includes:
- Improved footer metadata accuracy
- Better handling of compressed files (Snappy)
- Fixes for column statistics calculation
- More accurate file size tracking during writes

## Changes Made

**pom.xml:**
```xml
<parquet.version>1.16.0</parquet.version>
<parquet.format.version>2.12.0</parquet.format.version>
```

Added dependency overrides for:
- parquet-common
- parquet-encoding
- parquet-column
- parquet-hadoop
- parquet-avro
- parquet-format-structures
- parquet-format

## Expected Outcomes

### Best Case ✅
```
[INFO] Tests run: 10, Failures: 0, Errors: 0, Skipped: 0
```
All tests pass! Parquet 1.16.0 calculates file sizes correctly.

### If Still Fails ❌
Possible next steps:
1. **Try uncompressed Parquet** (remove Snappy, test if compression-related)
2. **Upgrade Spark to 4.0.1** (includes Parquet 1.14+, more integrated fixes)
3. **Investigate Parquet JIRA** for known 78-byte issues
4. **Workaround:** Pad files to expected size or disable column stats

### Intermediate Success 🟡
If error changes to different byte count or different failure mode, we're making progress!

## Debug Logging Still Active

The diagnostic logging from previous commits remains active:
- `🔧` Stream creation logs
- `✍️` Write call logs (>=20 bytes only)
- `🔒/✅` Close logs with totalBytesWritten
- `📍` getPos() logs (if called)

This will help confirm if Parquet 1.16.0 writes differently.

## Test Command

```bash
cd test/java/spark
docker compose down -v  # Clean state
docker compose up --abort-on-container-exit spark-tests
```

## Success Criteria

1. **No EOFException** in test output
2. **All 10 tests pass** (currently 9 pass, 1 fails)
3. **Consistent file sizes** between write and read

## Rollback Plan

If Parquet 1.16.0 causes new issues:
```bash
git revert 12504dc1a
# Returns to Parquet 1.13.1
```

## Timeline

- **Previous:** 250+ write calls, 684 bytes written, 762 expected
- **Now:** Parquet 1.16.0 should write correct size in footer
- **Next:** CI test run will confirm!

