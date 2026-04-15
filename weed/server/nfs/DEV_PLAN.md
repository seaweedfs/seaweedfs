# NFS Dev Plan

Status: active
Owner: codex

## Goal

Implement SeaweedFS NFS support as a filer-native gateway with stable object
identity, shared lock state, and restart-safe filehandles.

## Phase 0: architecture

- [x] Write the design note in [DESIGN.md](./DESIGN.md)
- [x] Choose filer-native gateway as the primary architecture
- [x] Identify stable inode/filehandle support as the first prerequisite

## Phase 1: filer identity foundation

- [x] Add server-side inode assignment for newly created filer entries
- [x] Preserve inode across in-place updates
- [x] Backfill inode on update for legacy zero-inode entries
- [x] Cover auto-created parent directories with the same inode assignment path
- [x] Add a filer-side inode secondary index foundation (`inode -> current path`)
- [x] Expose internal inode lookup helpers for future NFS handle resolution
- [x] Extend the inode index to cover multi-path hardlinks
- [x] Add generation-bearing handle metadata to the inode index

## Phase 2: reusable filer-backed filesystem core

- [x] Add a minimal filer-backed read-only filesystem adapter for NFS
- [x] Add filer-backed metadata mutations and small inline-content writes for the experimental NFS adapter
- [x] Extract shared read/write helpers from mount, WebDAV, and SFTP
  (first pass landed: `filer.SaveGatewayDataAsChunk` is the shared
  AssignVolume + volume-server upload helper used by both the NFS and
  WebDAV gateways. The filer chunk-read path —
  `filer.NonOverlappingVisibleIntervals` + `filer.ViewFromVisibleIntervals`
  + `filer.NewChunkReaderAtFromClient` — was already shared. mount keeps
  its own saveDataAsChunk because of the fileId pool and chunkCache
  write-through, both of which are mount-specific)
- [x] Standardize direct-volume read mode vs filer-proxy mode
- [x] Reuse chunk cache and mutation stream helpers without FUSE dependencies
  (NFS server imports `weed/filer.ReaderCache` and `weed/util/chunk_cache`
  directly, with no `weed/mount` or go-fuse imports)

## Phase 3: NFS frontend

- [x] Add `weed nfs` command and option surface
- [x] Add deterministic filehandle codec and inode lookup plumbing in the NFS skeleton
- [x] Integrate an experimental read-only NFSv3 RPC frontend
- [x] Implement initial metadata operations against filer RPCs
- [x] Implement initial namespace mutations and small-file inline writes for the experimental server
- [x] Implement initial buffered large-file writes through `AssignVolume` and volume-server chunk uploads
- [x] Expand direct data-path reads/writes through volume servers beyond the current buffered fallback
  (streaming WRITE path landed: each NFS WRITE RPC now issues one
  `AssignVolume` + one chunk upload + one filer `UpdateEntry`. No
  whole-file buffer, no per-file size cap; small writes that fit the
  4 MiB inline budget still take the inline fast path, matching the
  filer HTTP handler's `SaveToFilerLimit` shortcut)
- [x] Add export configuration and basic access controls

## Phase 4: HA correctness

- [x] Deterministic filehandle format based on filer-owned identity
- [ ] Shared lock state and reclaim/grace handling
  (blocked upstream — the `github.com/willscott/go-nfs` backend has no
  NLM or NFSv4 lock state RPCs, so any shared-lock implementation needs
  either a library change or an NFSv4 migration. See "Current Blockers")
- [x] Metadata subscription-based invalidation in each NFS head
- [x] Multi-head restart and failover tests

## Validation

- [x] Filer unit tests for inode assignment and preservation
- [x] Filer unit tests for hardlink-aware inode index updates
- [x] NFS unit tests for filehandle encoding and inode-based resolution
- [x] NFS unit tests for read-only mount, getattr/lookup handle round trips, readdir, and inline reads
- [x] NFS unit tests for create/write/truncate/rename/remove metadata flows in the experimental adapter
- [x] User-space protocol integration tests for create/read/write/rename/delete over NFS
- [x] Stale-handle tests after delete/recreate
- [x] Hardlink and symlink tests
- [x] Restart and failover tests with shared filer state
- [x] Read-only export and mount allowlist tests
- [x] Metadata subscription invalidation tests
- [ ] Lock tests
  (blocked by the same upstream gap — without shared lock state there is
  nothing to test; will land with the lock implementation PR)

## Current Blockers

- Shared NFS lock semantics are not implementable on the current
  `github.com/willscott/go-nfs` backend. It exposes local billy
  `File.Lock`/`Unlock` hooks, but does not implement the NLM or NFSv4 lock
  state RPCs needed for shared lock state, grace periods, or reclaim after
  failover.

## Current PR Scope

This first PR lands the first two enabling slices needed before an NFS
frontend can be credible:

- design and development plan documents
- server-side inode assignment
- filer-side inode index foundation, multi-path hardlink support, generation metadata, and tests
- `weed nfs` command, experimental `go-nfs` server path, deterministic filehandle/lookup plumbing, and filer-backed namespace mutations
- buffered large-file chunk uploads through `AssignVolume`
- inode preservation/backfill tests
- hardlink/symlink protocol coverage and restart-safe handle tests
- read-only export mode and client allowlist enforcement for `weed nfs`
- shared reader-cache invalidation from filer metadata subscriptions
- mount-style lookup routing for `direct`, `publicUrl`, and `filerProxy` chunk reads
- explicit lock-protocol blocker documentation for the current `go-nfs` stack
