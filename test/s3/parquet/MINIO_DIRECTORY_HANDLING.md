# MinIO Directory Handling Comparison

## Overview

This document compares how MinIO handles directory markers versus SeaweedFS's implementation, and explains the different approaches to S3 directory semantics.

## MinIO's Approach

MinIO handles implicit directories similarly to AWS S3:

1. **No explicit directory objects**: Directories are implicit, defined only by object key prefixes
2. **HEAD on directory returns 404**: Consistent with AWS S3 behavior
3. **LIST operations reveal directories**: Directories are discovered through delimiter-based LIST operations
4. **Automatic prefix handling**: MinIO automatically recognizes prefixes as directories

### MinIO Implementation Details

- Uses in-memory metadata for fast prefix lookups
- Optimized for LIST operations with common delimiter (`/`)
- No persistent directory objects in storage layer
- Directories "exist" as long as they contain objects

## SeaweedFS Approach

SeaweedFS uses a filer-based approach with real directory entries:

### Before the Fix

1. **Explicit directory objects**: Could create 0-byte objects as directory markers
2. **HEAD returns 200**: Even for implicit directories
3. **Caused s3fs issues**: s3fs interpreted 0-byte HEAD responses as empty files

### After the Fix

1. **Hybrid approach**: Supports both explicit markers (with `/` suffix) and implicit directories
2. **HEAD returns 404 for implicit directories**: Matches AWS S3 and MinIO behavior
3. **Filer integration**: Uses filer's directory metadata to detect implicit directories
4. **s3fs compatibility**: Triggers proper LIST fallback behavior

## Key Differences

| Aspect | MinIO | SeaweedFS (After Fix) |
|--------|-------|----------------------|
| Directory Storage | No persistent objects | Filer directory entries |
| Implicit Directory HEAD | 404 Not Found | 404 Not Found |
| Explicit Marker HEAD | Not applicable | 200 OK (with `/` suffix) |
| Child Detection | Prefix scan | Filer LIST operation |
| Performance | In-memory lookups | Filer gRPC calls |

## Implementation Considerations

### Advantages of SeaweedFS Approach
- Integrates with existing filer metadata
- Supports both implicit and explicit directories
- Preserves directory metadata and attributes
- Compatible with POSIX filer semantics

### Trade-offs
- Additional filer communication overhead for HEAD requests
- Complexity of supporting both directory paradigms
- Performance depends on filer efficiency

## TODO

- [ ] Add performance benchmark comparison: MinIO vs SeaweedFS
- [ ] Document edge cases where behaviors differ
- [ ] Add example request/response traces for both systems
- [ ] Document migration path for users moving from MinIO to SeaweedFS
- [ ] Add compatibility matrix for different S3 clients

