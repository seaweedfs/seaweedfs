# S3 Storage Class to Disk Routing

## Problem
SeaweedFS already stores S3 `x-amz-storage-class` as object metadata, but write allocation (`AssignVolume`) does not use it. Objects are therefore not routed to specific disk tags by storage class.

## Goals
1. Route new writes to disk types based on storage class.
2. Preserve current behavior when no routing map is configured.
3. Keep implementation incremental so future storage-class transitions can reuse the same decision logic.

## Phase 1 (implemented in this PR)
### Scope
1. Configure routing in `filer.toml`:
   - section: `[s3.storage_class_disk_type]`
   - keys: lowercase storage classes (e.g. `standard_ia = "hdd"`).
2. Load this routing map from filer via gRPC (`GetFilerConfiguration`) so S3 instances use filer-provided config.
3. Provide explicit defaults (when not set in `filer.toml`):
   - `standard = "ssd"`
   - all colder classes default to `"hdd"`.
4. Resolve effective storage class from:
   - request header `X-Amz-Storage-Class`
   - fallback to stored entry metadata (when available)
   - fallback to `STANDARD`
5. Apply mapped disk type on `AssignVolume` for `putToFiler` upload path.
6. For multipart uploads, propagate storage class from upload metadata to part requests so part chunk allocation also follows routing.

### Behavior
1. If class mapping is not configured explicitly in `filer.toml`, filer defaults are applied.
2. If class is unknown to the routing table at runtime: unchanged behavior (`DiskType=""`).
3. Invalid storage class in request header: return `InvalidStorageClass`.
4. Metadata storage remains AWS-compatible (`X-Amz-Storage-Class` is still saved when explicitly provided).

## Phase 2 (next)
1. Apply the same routing decision to server-side copy chunk allocation paths.
2. Ensure storage-class changes via copy (`x-amz-metadata-directive: REPLACE` + new class) move chunks to target disk type immediately.

## Phase 3 (future)
1. Add async background transition API for in-place class change:
   - mark object transition intent in metadata
   - enqueue migration job
   - copy chunks to target class disk
   - atomically swap metadata/chunks
   - garbage collect old chunks
2. Add transition job status and retry handling.
3. Add bucket policy controls for allowed transitions.

## Non-goals for Phase 1
1. Lifecycle-driven transitions (`STANDARD` -> `GLACIER` by age).
2. Cost-aware placement balancing.
3. Cross-cluster migration.
