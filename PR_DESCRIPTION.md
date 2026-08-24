# EC decode: read shards with the encode-time block layout

Fixes #3854

## Problem

`ec_encoder` and `ec_decoder` were using inconsistent definitions of the `.dat` file size to decide the block layout of erasure-coded shards.

- **Encoder** (`ec_encoder.go`) used the actual physical size of the `.dat` file.
- **Decoder** (`ec_decoder.go`) recomputed a "live extent" by iterating the `.ecx` index and skipping deleted entries, taking the maximum `offset + actual_size` of the remaining live needles.

After tail deletions, the live extent can be significantly smaller than the physical `.dat` size. This is usually harmless because the decoder only needs to reconstruct live needles. However, once the gap between the two sizes crosses a **1 GB boundary**, the layouts diverge:

- The encoder writes one full large block (1 GB per data shard, 10 GB total row).
- The decoder, seeing a live extent smaller than 10 GB, expects only small blocks (1 MB per shard) and reads the shards in the wrong order.

The result is corrupted decode output or a failure to reconstruct the volume.

## Root cause

`FindDatFileSize` returned the live data extent, but `WriteDatFile` used that value to drive the large-block / small-block row loop. The row layout is determined at **encode time** by the physical `.dat` size, so it must be reproduced at decode time from the same value.

## Fix

This PR makes the decode path use the **encode-time `.dat` size** for the shard block layout, while still using the live extent as the final write length.

### Changes

1. **Record the encode-time size in `.vif`** (`weed/worker/tasks/erasure_coding/ec_task.go`)
   - When encoding a volume, store the original `.dat` size in `VolumeInfo.EcShardConfig.EncodedDatFileSize`.

2. **Pass the encode-time size through the decode API** (`weed/server/volume_grpc_erasure_coding.go`, `weed/command/fix.go`)
   - `EcShardRebuildRequest` and `EcVolume` now carry the encoded size so the decoder can use it directly.

3. **Use the encode-time size in `WriteDatFile`** (`weed/storage/erasure_coding/ec_decoder.go`)
   - `WriteDatFile` now accepts both `datFileSize` (live extent, still the amount to write) and `encodedDatFileSize` (the layout ruler).
   - The large-block row loop is driven by `encodedDatFileSize`.
   - The remaining `datFileSize` bytes are copied from the correct shard positions.

4. **Fallback for legacy volumes without the encoded size**
   - If the `.vif` does not contain the encode-time size, derive it from the physical shard size.
   - Detect and reject ambiguous shard sizes (e.g., a shard that is an exact multiple of the large block size), because the layout cannot be uniquely reconstructed. Users must re-encode such volumes to get the `.vif` metadata.

5. **Rust volume server parity** (`seaweed-volume/src/storage/erasure_coding/ec_decoder.rs`, `ec_encoder.rs`)
   - The same fix is applied to the Rust implementation so both Go and Rust servers behave identically.

6. **Regression tests** (`weed/storage/erasure_coding/ec_roundtrip_test.go`, `seaweed-volume/...`)
   - Encode a volume whose size is just above a large-block row boundary, delete needles at the tail to shrink the live extent below the boundary, then decode and verify the remaining needles are intact.

## Verification

```bash
cd weed/storage/erasure_coding
go test -run TestEcDecode -v ./...

# or full EC roundtrip tests
go test ./...
```

Rust side (if the Rust toolchain is set up):

```bash
cd seaweed-volume
cargo test erasure_coding
```

All tests pass, and decode after tail deletions now preserves the original block layout.
